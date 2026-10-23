//! HTTP and native-client qualification for LFS locks and OCI Distribution.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::shadow_unrelated
)]

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server::{ObjectStorageAdapter, ServerConfig, ServerFrontend, app};
use shardline_server_core::{AuthProvider, LocalHmacProvider};
use shardline_storage::S3ObjectStoreConfig;
use std::{num::NonZeroUsize, path::Path};

const KEY: &[u8] = b"protocol-completion-signing-key-32!";
const INDEX: &str = "application/vnd.oci.image.index.v1+json";

mod copy_parts;
mod git_atomic;
mod lfs_errors;
mod multipart_listing;
mod oci_conformance;
mod oci_index;
mod s3_attributes;
#[path = "protocol_completion/s3_post_object.rs"]
mod s3_post_object;
mod signed_streaming;

#[tokio::test]
#[ignore = "requires BOTOCORE_PYTHON pointing to a native botocore SDK installation"]
async fn native_botocore_unsigned_checksum_trailers() {
    let root = tempfile::tempdir().unwrap();
    let credential = token("writer", "aws-resume", TokenScope::Write);
    let server = start_configured(
        root.path(),
        ServerFrontend::S3,
        true,
        None,
        None,
        Some(signing_identity(credential)),
    )
    .await;
    let endpoint = server.base.clone();
    let python = std::env::var("BOTOCORE_PYTHON").expect("BOTOCORE_PYTHON must be configured");
    let output = tokio::task::spawn_blocking(move || std::process::Command::new(python).arg("-c").arg(r#"
import io, sys, urllib.request, urllib.error, urllib.parse
import botocore.session
from botocore.config import Config
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from botocore.httpchecksum import AwsChunkedWrapper, Crc32Checksum, Sha1Checksum, Sha256Checksum
url = sys.argv[1] + '/aws-resume.locks/native-trailers.bin'
payload = b'native unsigned checksum trailer bytes' * 200
credentials = Credentials('SIGNINGKEY', 'legacy-token-bridge')
def send(method, wire=None, trailer=None, target=None):
    target = target or url
    headers = {}
    if trailer:
        headers = {'Content-Encoding':'aws-chunked', 'x-amz-decoded-content-length':str(len(payload)), 'x-amz-trailer':trailer}
    request = AWSRequest(method=method, url=target, data=wire, headers=headers)
    if trailer:
        request.context['checksum'] = {'request_algorithm':{'in':'trailer'}}
    S3SigV4Auth(credentials, 's3', 'us-east-1').add_auth(request)
    if trailer:
        assert request.headers['X-Amz-Content-SHA256'] == 'STREAMING-UNSIGNED-PAYLOAD-TRAILER'
    return urllib.request.urlopen(urllib.request.Request(target, data=wire, headers=dict(request.headers.items()), method=method))
client = botocore.session.get_session().create_client('s3', endpoint_url=sys.argv[1], region_name='us-east-1', aws_access_key_id='SIGNINGKEY', aws_secret_access_key='legacy-token-bridge', config=Config(signature_version='s3v4', retries={'max_attempts':0}))
for algorithm, checksum in [('crc32', Crc32Checksum), ('sha1', Sha1Checksum), ('sha256', Sha256Checksum)]:
    trailer = 'x-amz-checksum-' + algorithm
    wire = AwsChunkedWrapper(io.BytesIO(payload), checksum_cls=checksum, checksum_name=trailer, chunk_size=7).read()
    assert send('PUT', wire, trailer).status == 200
    assert send('GET').read() == payload
    corrupt = bytearray(wire); corrupt[5] ^= 1
    for invalid in [bytes(corrupt), wire[:-2], wire+b'x']:
        try:
            send('PUT', invalid, trailer)
            raise AssertionError('invalid trailer upload accepted')
        except urllib.error.HTTPError as error:
            assert error.code == (400 if invalid == bytes(corrupt) else 403)
            if error.code == 400:
                assert b'<Code>BadDigest</Code>' in error.read()
        assert send('GET').read() == payload
    key = 'native-trailer-part-' + algorithm + '.bin'
    upload = client.create_multipart_upload(Bucket='aws-resume.locks', Key=key)['UploadId']
    part_url = sys.argv[1] + '/aws-resume.locks/' + key + '?' + urllib.parse.urlencode({'uploadId':upload, 'partNumber':'1'})
    etag = send('PUT', wire, trailer, part_url).headers['ETag']
    try:
        send('PUT', bytes(corrupt), trailer, part_url)
        raise AssertionError('corrupt multipart overwrite accepted')
    except urllib.error.HTTPError as error:
        assert error.code == 400
        assert b'<Code>BadDigest</Code>' in error.read()
    client.complete_multipart_upload(Bucket='aws-resume.locks', Key=key, UploadId=upload, MultipartUpload={'Parts':[{'PartNumber':1,'ETag':etag}]})
    assert client.get_object(Bucket='aws-resume.locks', Key=key)['Body'].read() == payload
"#).arg(endpoint).output()).await.unwrap().unwrap();
    assert!(
        output.status.success(),
        "native trailer SDK failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    server.stop().await;
}

#[tokio::test]
#[ignore = "requires BOTOCORE_PYTHON pointing to a native botocore SDK installation"]
async fn native_botocore_session_credentials_and_presigned_put() {
    exercise_botocore_session_credentials(None, None).await;
}

#[tokio::test]
#[ignore = "requires native BOTOCORE_PYTHON and disposable PostgreSQL DATABASE_URL"]
async fn native_botocore_session_credentials_on_postgres() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL must be configured");
    exercise_botocore_session_credentials(Some(&database), None).await;
}

#[tokio::test]
#[ignore = "requires native BOTOCORE_PYTHON, disposable PostgreSQL DATABASE_URL and TEST_S3_* MinIO storage"]
async fn native_botocore_full_object_checksum_survives_postgres_minio_restart() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL must be configured");
    let prefix = format!(
        "protocol-full-checksum-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    );
    let storage = S3ObjectStoreConfig::new(
        std::env::var("TEST_S3_BUCKET").expect("TEST_S3_BUCKET must be configured"),
        "us-east-1".to_owned(),
    )
    .with_endpoint(Some(
        std::env::var("TEST_S3_ENDPOINT").expect("TEST_S3_ENDPOINT must be configured"),
    ))
    .with_allow_http(true)
    .with_credentials(
        Some(shardline_protocol::SecretString::new(
            std::env::var("TEST_S3_ACCESS_KEY").expect("TEST_S3_ACCESS_KEY must be configured"),
        )),
        Some(shardline_protocol::SecretString::new(
            std::env::var("TEST_S3_SECRET_KEY").expect("TEST_S3_SECRET_KEY must be configured"),
        )),
        None,
    )
    .with_key_prefix(Some(&prefix));
    exercise_botocore_session_credentials(Some(&database), Some(storage)).await;
}

async fn exercise_botocore_session_credentials(
    database: Option<&str>,
    storage: Option<S3ObjectStoreConfig>,
) {
    let root = tempfile::tempdir().unwrap();
    let credential = token("writer", "aws-resume", TokenScope::Write);
    let signing = signing_identity(credential.clone())
        .with_session_token(shardline_protocol::SecretString::new(
            "fixture-session-token".to_owned(),
        ))
        .unwrap();
    let server = start_configured(
        root.path(),
        ServerFrontend::S3,
        true,
        database,
        storage.clone(),
        Some(signing),
    )
    .await;
    let python = std::env::var("BOTOCORE_PYTHON").expect("BOTOCORE_PYTHON must be configured");
    let restart_python = python.clone();
    let endpoint = server.base.clone();
    // Execute the native SDK signer and HTTP client. Python is only the
    // external client under test; Rust owns server setup and assertions.
    let output = tokio::task::spawn_blocking(move || std::process::Command::new(python)
        .arg("-c").arg(r#"
import base64, datetime, hashlib, json, sys, zlib
import botocore.session
from botocore.config import Config
from botocore.exceptions import ClientError
import base64
def crc64nvme(payload):
    polynomial = 0x9A6C9329AC4BC9B5
    value = 0xFFFFFFFFFFFFFFFF
    for byte in payload:
        value ^= byte
        for _ in range(8):
            value = (value >> 1) ^ (polynomial if value & 1 else 0)
    value ^= 0xFFFFFFFFFFFFFFFF
    return base64.b64encode(value.to_bytes(8, 'big')).decode()
def crc32c(payload):
    value = 0xFFFFFFFF
    for byte in payload:
        value ^= byte
        for _ in range(8):
            value = (value >> 1) ^ (0x82F63B78 if value & 1 else 0)
    value ^= 0xFFFFFFFF
    return base64.b64encode(value.to_bytes(4, 'big')).decode()
def checksum_for(algorithm, payload):
    if algorithm == 'CRC32':
        return base64.b64encode(zlib.crc32(payload).to_bytes(4, 'big')).decode()
    if algorithm == 'CRC32C':
        return crc32c(payload)
    if algorithm in ('SHA1', 'SHA256'):
        return base64.b64encode(hashlib.new(algorithm.lower(), payload).digest()).decode()
    return crc64nvme(payload)
def checksum_field(algorithm):
    return 'Checksum' + algorithm
def client(token):
    return botocore.session.get_session().create_client('s3', endpoint_url=sys.argv[1], region_name='us-east-1', aws_access_key_id='SIGNINGKEY', aws_secret_access_key='legacy-token-bridge', aws_session_token=token, config=Config(signature_version='s3v4', retries={'max_attempts': 0}))
c = client('fixture-session-token')
versioning = c.get_bucket_versioning(Bucket='aws-resume.locks')
assert 'Status' not in versioning, versioning
for operation in (
    lambda: c.put_bucket_versioning(Bucket='aws-resume.locks', VersioningConfiguration={'Status': 'Enabled'}),
    lambda: c.list_object_versions(Bucket='aws-resume.locks'),
):
    try:
        operation()
        raise AssertionError('unsupported version history operation was accepted')
    except ClientError as error:
        assert error.response['ResponseMetadata']['HTTPStatusCode'] == 501, error.response
created = []
try:
    for index in range(41):
        key = 'paginator/' + ('same.bin' if index % 3 == 0 else f'{index:03d}.bin')
        upload = c.create_multipart_upload(Bucket='aws-resume.locks', Key=key)
        created.append((key, upload['UploadId']))
    for page_size in (1, 3, 7):
        seen = []
        pages = 0
        for page in c.get_paginator('list_multipart_uploads').paginate(Bucket='aws-resume.locks', Prefix='paginator/', PaginationConfig={'PageSize': page_size}):
            pages += 1
            assert pages <= len(created), 'pagination failed to advance'
            entries = page.get('Uploads', [])
            assert len(entries) <= page_size
            seen.extend((entry['Key'], entry['UploadId']) for entry in entries)
        assert len(seen) == len(set(seen)) == len(created), (page_size, seen)
        assert set(seen) == set(created), (page_size, seen)
    first_batch = c.get_paginator('list_multipart_uploads').paginate(Bucket='aws-resume.locks', Prefix='paginator/', PaginationConfig={'PageSize': 7, 'MaxItems': 10}).build_full_result()
    assert len(first_batch['Uploads']) == 10 and first_batch.get('NextToken')
    remaining = c.get_paginator('list_multipart_uploads').paginate(Bucket='aws-resume.locks', Prefix='paginator/', PaginationConfig={'PageSize': 7, 'StartingToken': first_batch['NextToken']}).build_full_result()
    resumed = [(entry['Key'], entry['UploadId']) for entry in first_batch['Uploads'] + remaining['Uploads']]
    assert len(resumed) == len(set(resumed)) == len(created)
    assert set(resumed) == set(created)
finally:
    for key, upload_id in created:
        c.abort_multipart_upload(Bucket='aws-resume.locks', Key=key, UploadId=upload_id)
c.put_object(Bucket='aws-resume.locks', Key='session/header.bin', Body=b'header-signed session bytes', ContentType='application/x-shardline-test')
assert c.get_object(Bucket='aws-resume.locks', Key='session/header.bin')['Body'].read() == b'header-signed session bytes'
assert c.head_object(Bucket='aws-resume.locks', Key='session/header.bin')['ContentType'] == 'application/x-shardline-test'
overridden = c.get_object(Bucket='aws-resume.locks', Key='session/header.bin', ResponseContentType='text/plain', ResponseCacheControl='no-cache', ResponseContentDisposition='inline', ResponseContentEncoding='identity', ResponseContentLanguage='en-US', ResponseExpires=datetime.datetime(2015, 10, 21, 7, 28, tzinfo=datetime.timezone.utc))
assert overridden['ContentType'] == 'text/plain' and overridden['CacheControl'] == 'no-cache', overridden
assert overridden['ContentDisposition'] == 'inline' and overridden['ContentEncoding'] == 'identity' and overridden['ContentLanguage'] == 'en-US', overridden
assert overridden['Expires'] == datetime.datetime(2015, 10, 21, 7, 28, tzinfo=datetime.timezone.utc), overridden
assert overridden['Body'].read() == b'header-signed session bytes'
overridden_head = c.head_object(Bucket='aws-resume.locks', Key='session/header.bin', ResponseContentDisposition='attachment')
assert overridden_head['ContentDisposition'] == 'attachment', overridden_head
partial = c.get_object(Bucket='aws-resume.locks', Key='session/header.bin', Range='bytes=0-3', ResponseContentType='text/plain')
assert partial['ResponseMetadata']['HTTPStatusCode'] == 206 and partial['ContentType'] != 'text/plain', partial
c.put_object_tagging(Bucket='aws-resume.locks', Key='session/header.bin', Tagging={'TagSet': [
    {'Key': 'environment', 'Value': 'integration'}, {'Key': 'owner', 'Value': 'shardline'}]})
assert c.get_object_tagging(Bucket='aws-resume.locks', Key='session/header.bin')['TagSet'] == [
    {'Key': 'environment', 'Value': 'integration'}, {'Key': 'owner', 'Value': 'shardline'}]
attributes = c.get_object_attributes(Bucket='aws-resume.locks', Key='session/header.bin', ObjectAttributes=['ETag', 'ObjectSize', 'StorageClass', 'Checksum'])
assert attributes['ObjectSize'] == len(b'header-signed session bytes')
assert attributes['ETag'] == c.head_object(Bucket='aws-resume.locks', Key='session/header.bin')['ETag'].strip('"')
assert 'StorageClass' not in attributes
assert attributes['Checksum']['ChecksumType'] == 'FULL_OBJECT', attributes
assert 'ChecksumCRC32' in attributes['Checksum'] and 'ObjectParts' not in attributes, attributes
size_only = c.get_object_attributes(Bucket='aws-resume.locks', Key='session/header.bin', ObjectAttributes=['ObjectSize'])
assert size_only['ObjectSize'] == attributes['ObjectSize'] and 'ETag' not in size_only and 'StorageClass' not in size_only
upload = c.create_multipart_upload(Bucket='aws-resume.locks', Key='session/multipart.bin', ChecksumAlgorithm='CRC32')
part = c.upload_part(Bucket='aws-resume.locks', Key='session/multipart.bin', UploadId=upload['UploadId'], PartNumber=1, Body=b'multipart attribute part')
assert 'ChecksumCRC32' in part, part
c.complete_multipart_upload(Bucket='aws-resume.locks', Key='session/multipart.bin', UploadId=upload['UploadId'], MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part['ETag'], 'ChecksumCRC32': part['ChecksumCRC32']}]})
multipart_attributes = c.get_object_attributes(Bucket='aws-resume.locks', Key='session/multipart.bin', ObjectAttributes=['Checksum', 'ObjectParts'])
assert multipart_attributes['ObjectParts']['TotalPartsCount'] == 1, multipart_attributes
assert multipart_attributes['Checksum']['ChecksumType'] == 'COMPOSITE', multipart_attributes
assert multipart_attributes['ObjectParts']['Parts'] == [{'PartNumber': 1, 'Size': len(b'multipart attribute part'), 'ChecksumCRC32': part['ChecksumCRC32']}], multipart_attributes

# Verify SDK request/response pagination for committed multipart attributes.
page_key = 'session/attribute-pages.bin'
page_upload = c.create_multipart_upload(Bucket='aws-resume.locks', Key=page_key, ChecksumAlgorithm='CRC32')
page_parts = []
page_payloads = (b'a' * (5 * 1024 * 1024), b'second attribute part')
for number, payload in enumerate(page_payloads, start=1):
    checksum = checksum_for('CRC32', payload)
    result = c.upload_part(Bucket='aws-resume.locks', Key=page_key, UploadId=page_upload['UploadId'], PartNumber=number, Body=payload, ChecksumCRC32=checksum)
    page_parts.append({'PartNumber': number, 'ETag': result['ETag'], 'ChecksumCRC32': checksum})
c.complete_multipart_upload(Bucket='aws-resume.locks', Key=page_key, UploadId=page_upload['UploadId'], MultipartUpload={'Parts': page_parts})
first_page = c.get_object_attributes(Bucket='aws-resume.locks', Key=page_key, ObjectAttributes=['ObjectParts'], MaxParts=1)
first_object_parts = first_page['ObjectParts']
assert first_object_parts['MaxParts'] == 1 and first_object_parts['PartNumberMarker'] == 0, first_object_parts
assert first_object_parts['IsTruncated'] and first_object_parts['NextPartNumberMarker'] == 1, first_object_parts
assert first_object_parts['Parts'] == [{'PartNumber': 1, 'Size': len(page_payloads[0]), 'ChecksumCRC32': page_parts[0]['ChecksumCRC32']}], first_object_parts
second_page = c.get_object_attributes(Bucket='aws-resume.locks', Key=page_key, ObjectAttributes=['ObjectParts'], MaxParts=1, PartNumberMarker=first_object_parts['NextPartNumberMarker'])
second_object_parts = second_page['ObjectParts']
assert second_object_parts['MaxParts'] == 1 and second_object_parts['PartNumberMarker'] == 1, second_object_parts
assert not second_object_parts['IsTruncated'] and 'NextPartNumberMarker' not in second_object_parts, second_object_parts
assert second_object_parts['Parts'] == [{'PartNumber': 2, 'Size': len(page_payloads[1]), 'ChecksumCRC32': page_parts[1]['ChecksumCRC32']}], second_object_parts
for algorithm in ('CRC32', 'CRC32C', 'CRC64NVME'):
    field = checksum_field(algorithm)
    key = 'session/full-object-' + algorithm.lower() + '.bin'
    payload = ('native botocore FULL_OBJECT ' + algorithm).encode()
    expected = checksum_for(algorithm, payload)
    upload = c.create_multipart_upload(Bucket='aws-resume.locks', Key=key, ChecksumAlgorithm=algorithm, ChecksumType='FULL_OBJECT', ContentType='application/x-shardline-full-object')
    part_checksum = checksum_for(algorithm, payload)
    part = c.upload_part(Bucket='aws-resume.locks', Key=key, UploadId=upload['UploadId'], PartNumber=1, Body=payload, **{field: part_checksum})
    listing = c.list_parts(Bucket='aws-resume.locks', Key=key, UploadId=upload['UploadId'])
    assert listing['ChecksumAlgorithm'] == algorithm and listing['ChecksumType'] == 'FULL_OBJECT', listing
    assert listing['Parts'][0][field] == part_checksum, listing
    completed = c.complete_multipart_upload(Bucket='aws-resume.locks', Key=key, UploadId=upload['UploadId'], MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part['ETag'], field: part_checksum}]}, **{field: expected, 'ChecksumType': 'FULL_OBJECT'})
    assert completed[field] == expected, completed
    attributes = c.get_object_attributes(Bucket='aws-resume.locks', Key=key, ObjectAttributes=['Checksum', 'ObjectParts'])
    assert attributes['Checksum'] == {field: expected, 'ChecksumType': 'FULL_OBJECT'}, attributes
    assert attributes['ObjectParts']['TotalPartsCount'] == 1, attributes
    read = c.get_object(Bucket='aws-resume.locks', Key=key, ChecksumMode='ENABLED')
    assert read[field] == expected and read['Body'].read() == payload, read
    assert c.head_object(Bucket='aws-resume.locks', Key=key)['ContentType'] == 'application/x-shardline-full-object'
for algorithm in ('CRC32C', 'SHA1', 'SHA256'):
    field = checksum_field(algorithm)
    key = 'session/composite-' + algorithm.lower() + '.bin'
    payload = ('native botocore COMPOSITE ' + algorithm).encode()
    part_checksum = checksum_for(algorithm, payload)
    upload = c.create_multipart_upload(Bucket='aws-resume.locks', Key=key, ChecksumAlgorithm=algorithm, ChecksumType='COMPOSITE')
    part = c.upload_part(Bucket='aws-resume.locks', Key=key, UploadId=upload['UploadId'], PartNumber=1, Body=payload, **{field: part_checksum})
    listing = c.list_parts(Bucket='aws-resume.locks', Key=key, UploadId=upload['UploadId'])
    assert listing['ChecksumAlgorithm'] == algorithm and listing['ChecksumType'] == 'COMPOSITE', listing
    assert listing['Parts'][0][field] == part_checksum, listing
    completed = c.complete_multipart_upload(Bucket='aws-resume.locks', Key=key, UploadId=upload['UploadId'], MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part['ETag'], field: part_checksum}]})
    expected = checksum_for(algorithm, base64.b64decode(part_checksum)) + '-1'
    attributes = c.get_object_attributes(Bucket='aws-resume.locks', Key=key, ObjectAttributes=['Checksum', 'ObjectParts'])
    assert attributes['Checksum'] == {field: expected, 'ChecksumType': 'COMPOSITE'}, attributes
    assert attributes['ObjectParts']['Parts'][0][field] == part_checksum, attributes
    read = c.get_object(Bucket='aws-resume.locks', Key=key, ChecksumMode='ENABLED')
    assert read[field] == expected and read['Body'].read() == payload, read
head = c.head_object(Bucket='aws-resume.locks', Key='session/header.bin')
for operation in (c.get_object, c.head_object):
    for condition, expected in (({'IfModifiedSince': head['LastModified']}, 304), ({'IfUnmodifiedSince': datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)}, 412)):
        try:
            operation(Bucket='aws-resume.locks', Key='session/header.bin', **condition)
            raise AssertionError('date condition ignored')
        except ClientError as error:
            assert error.response['ResponseMetadata']['HTTPStatusCode'] == expected
    allowed = operation(Bucket='aws-resume.locks', Key='session/header.bin', IfMatch=head['ETag'], IfUnmodifiedSince=datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc))
    if 'Body' in allowed:
        assert allowed['Body'].read() == b'header-signed session bytes'
for token in (None, 'wrong-session-token'):
    try:
        client(token).put_object(Bucket='aws-resume.locks', Key='session/header.bin', Body=b'changed')
        raise AssertionError('unbound session credential accepted')
    except ClientError as error:
        assert error.response['ResponseMetadata']['HTTPStatusCode'] == 403
params = {'Bucket': 'aws-resume.locks', 'Key': 'session/presigned.bin'}
print(json.dumps({'put': c.generate_presigned_url('put_object', Params=params, ExpiresIn=60), 'get': c.generate_presigned_url('get_object', Params=params, ExpiresIn=60), 'other': c.generate_presigned_url('put_object', Params={'Bucket': 'other.locks', 'Key': 'session/presigned.bin'}, ExpiresIn=60)}))
"#).arg(endpoint).output()).await.unwrap().unwrap();
    assert!(
        output.status.success(),
        "native botocore failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let urls: Value = serde_json::from_slice(&output.stdout).unwrap();
    let client = Client::new();
    let put = urls["put"].as_str().unwrap();
    let response = client
        .put(put)
        .body("presigned session bytes")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    for session_token in [None, Some("changed")] {
        let mut altered = reqwest::Url::parse(put).unwrap();
        let pairs = altered
            .query_pairs()
            .filter(|(key, _)| key != "X-Amz-Security-Token")
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();
        altered.set_query(None);
        altered.query_pairs_mut().extend_pairs(pairs);
        if let Some(session_token) = session_token {
            altered
                .query_pairs_mut()
                .append_pair("X-Amz-Security-Token", session_token);
        }
        let response = client.put(altered).body("changed").send().await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
    let response = client
        .put(urls["other"].as_str().unwrap())
        .body("changed")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response = client
        .get(urls["get"].as_str().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.bytes().await.unwrap().as_ref(),
        b"presigned session bytes"
    );

    // Restart the process against the same index and object store, then use
    // Botocore again to prove the checksum proof and bytes survived process
    // replacement. In the PostgreSQL variant, the metadata also crossed a
    // database connection/process boundary.
    server.stop().await;
    let restarted_signing = signing_identity(credential)
        .with_session_token(shardline_protocol::SecretString::new(
            "fixture-session-token".to_owned(),
        ))
        .unwrap();
    let restarted = start_configured(
        root.path(),
        ServerFrontend::S3,
        true,
        database,
        storage,
        Some(restarted_signing),
    )
    .await;
    let restart_endpoint = restarted.base.clone();
    let restart_output = tokio::task::spawn_blocking(move || {
        std::process::Command::new(restart_python)
            .arg("-c")
            .arg(
                r#"
import sys
import botocore.session
from botocore.config import Config
c = botocore.session.get_session().create_client('s3', endpoint_url=sys.argv[1],
    region_name='us-east-1', aws_access_key_id='SIGNINGKEY',
    aws_secret_access_key='legacy-token-bridge', aws_session_token='fixture-session-token',
    config=Config(signature_version='s3v4', retries={'max_attempts': 0}))
assert c.get_object_tagging(Bucket='aws-resume.locks', Key='session/header.bin')['TagSet'] == [
    {'Key': 'environment', 'Value': 'integration'}, {'Key': 'owner', 'Value': 'shardline'}]
for algorithm in ('CRC32', 'CRC32C', 'CRC64NVME'):
    key = 'session/full-object-' + algorithm.lower() + '.bin'
    payload = ('native botocore FULL_OBJECT ' + algorithm).encode()
    field = 'Checksum' + algorithm
    attributes = c.get_object_attributes(Bucket='aws-resume.locks', Key=key, ObjectAttributes=['Checksum', 'ObjectParts'])
    assert attributes['Checksum']['ChecksumType'] == 'FULL_OBJECT', attributes
    assert attributes['Checksum'][field], attributes
    assert attributes['ObjectParts']['TotalPartsCount'] == 1, attributes
    response = c.get_object(Bucket='aws-resume.locks', Key=key, ChecksumMode='ENABLED')
    assert response['Body'].read() == payload, response
    assert response[field] == attributes['Checksum'][field], response
for algorithm in ('CRC32C', 'SHA1', 'SHA256'):
    key = 'session/composite-' + algorithm.lower() + '.bin'
    payload = ('native botocore COMPOSITE ' + algorithm).encode()
    field = 'Checksum' + algorithm
    attributes = c.get_object_attributes(Bucket='aws-resume.locks', Key=key, ObjectAttributes=['Checksum', 'ObjectParts'])
    assert attributes['Checksum']['ChecksumType'] == 'COMPOSITE', attributes
    assert attributes['Checksum'][field], attributes
    assert attributes['ObjectParts']['TotalPartsCount'] == 1, attributes
    response = c.get_object(Bucket='aws-resume.locks', Key=key, ChecksumMode='ENABLED')
    assert response['Body'].read() == payload, response
    assert response[field] == attributes['Checksum'][field], response
"#,
            )
            .arg(&restart_endpoint)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        restart_output.status.success(),
        "native botocore checksum reads after restart failed: {}",
        String::from_utf8_lossy(&restart_output.stderr)
    );
    restarted.stop().await;
}

fn signing_identity(bound_token: String) -> shardline_server::S3SigningIdentity {
    shardline_server::S3SigningIdentity::new(
        "SIGNINGKEY".to_owned(),
        shardline_protocol::SecretString::new("legacy-token-bridge".to_owned()),
        shardline_protocol::SecretString::new(bound_token),
        "us-east-1".to_owned(),
    )
    .unwrap()
}

fn signed_request(
    client: &Client,
    method: reqwest::Method,
    url: &str,
    committed_body: &[u8],
) -> reqwest::RequestBuilder {
    use hmac::{Hmac, Mac};
    let url = reqwest::Url::parse(url).unwrap();
    let now = chrono::Utc::now();
    let timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let host = format!("{}:{}", url.host_str().unwrap(), url.port().unwrap());
    let payload = hex::encode(Sha256::digest(committed_body));
    let signed = "host;x-amz-content-sha256;x-amz-date";
    let canonical = format!(
        "{method}\n{}\n\nhost:{host}\nx-amz-content-sha256:{payload}\nx-amz-date:{timestamp}\n\n{signed}\n{payload}",
        url.path()
    );
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let mut key = b"AWS4legacy-token-bridge".to_vec();
    for value in [&date, "us-east-1", "s3", "aws4_request", &to_sign] {
        let mut mac = Hmac::<Sha256>::new_from_slice(&key).unwrap();
        mac.update(value.as_bytes());
        key = mac.finalize().into_bytes().to_vec();
    }
    client.request(method, url).header("x-amz-date", timestamp).header("x-amz-content-sha256", payload)
        .header("authorization", format!("AWS4-HMAC-SHA256 Credential=SIGNINGKEY/{scope}, SignedHeaders={signed}, Signature={}", hex::encode(key)))
}

#[tokio::test]
async fn signed_s3_payload_tampering_cannot_replace_or_delete_data() {
    let root = tempfile::tempdir().unwrap();
    let credential = token("writer", "aws-resume", TokenScope::Write);
    let server = start_configured(
        root.path(),
        ServerFrontend::S3,
        true,
        None,
        None,
        Some(signing_identity(credential.clone())),
    )
    .await;
    let client = Client::new();
    let url = format!("{}/aws-resume.locks/object.bin", server.base);
    let response = signed_request(&client, reqwest::Method::PUT, &url, b"original")
        .body("original")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = signed_request(&client, reqwest::Method::PUT, &url, b"replacement")
        .body("tampered")
        .send()
        .await
        .unwrap();
    assert!(!response.status().is_success());
    let response = signed_request(&client, reqwest::Method::DELETE, &url, b"")
        .body("tampered")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response = client
        .get(&url)
        .bearer_auth(credential)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap().as_ref(), b"original");
}

#[tokio::test]
async fn signed_s3_identity_cannot_outlive_or_expand_its_bound_token() {
    let root = tempfile::tempdir().unwrap();
    let writer = token("writer", "aws-resume", TokenScope::Write);
    for (scope, expiry, can_read) in [
        (TokenScope::Read, u64::MAX, true),
        (TokenScope::Write, 1, false),
    ] {
        let repository =
            RepositoryScope::new(RepositoryProvider::Generic, "aws-resume", "locks", None).unwrap();
        let claims = TokenClaims::new("completion", "limited", scope, repository, expiry).unwrap();
        let bound = LocalHmacProvider::new(KEY)
            .unwrap()
            .mint_token(&claims)
            .unwrap();
        let server = start_configured(
            root.path(),
            ServerFrontend::S3,
            true,
            None,
            None,
            Some(signing_identity(bound)),
        )
        .await;
        let client = Client::new();
        let url = format!("{}/aws-resume.locks/object.bin", server.base);
        assert_eq!(
            client
                .put(&url)
                .bearer_auth(&writer)
                .body("original")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let response = signed_request(&client, reqwest::Method::PUT, &url, b"replacement")
            .body("replacement")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let response = signed_request(&client, reqwest::Method::GET, &url, b"")
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if can_read {
                StatusCode::OK
            } else {
                StatusCode::FORBIDDEN
            }
        );
        let response = client.get(&url).bearer_auth(&writer).send().await.unwrap();
        assert_eq!(response.bytes().await.unwrap().as_ref(), b"original");
    }
}

#[tokio::test]
#[ignore = "requires the native AWS CLI"]
async fn native_aws_cli_signing_identity_and_presigned_get() {
    let root = tempfile::tempdir().unwrap();
    let bound_token = token("writer", "aws-resume", TokenScope::Write);
    let signing = shardline_server::S3SigningIdentity::new(
        "SIGNINGKEY".to_owned(),
        shardline_protocol::SecretString::new("legacy-token-bridge".to_owned()),
        shardline_protocol::SecretString::new(bound_token.clone()),
        "us-east-1".to_owned(),
    )
    .unwrap();
    let server = start_configured(
        root.path(),
        ServerFrontend::S3,
        true,
        None,
        None,
        Some(signing),
    )
    .await;
    let file = root.path().join("signed.bin");
    std::fs::write(&file, b"signed native client payload").unwrap();
    aws_json(
        &server.base,
        "SIGNINGKEY",
        &["put-object", "--body", file.to_str().unwrap()],
    )
    .await;
    let config = root.path().join("aws-config");
    std::fs::write(&config, "[default]\ns3 =\n    signature_version = s3v4\n").unwrap();
    let endpoint = server.base.clone();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("aws")
            .args([
                "--endpoint-url",
                &endpoint,
                "--region",
                "us-east-1",
                "s3",
                "presign",
                "s3://aws-resume.locks/object.bin",
                "--expires-in",
                "60",
            ])
            .env("AWS_ACCESS_KEY_ID", "SIGNINGKEY")
            .env("AWS_SECRET_ACCESS_KEY", "legacy-token-bridge")
            .env("AWS_CONFIG_FILE", config)
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env_remove("AWS_SESSION_TOKEN")
            .output()
    })
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let url = String::from_utf8(output.stdout).unwrap().trim().to_owned();
    assert!(url.contains("X-Amz-Signature="));
    let client = Client::new();
    let response = client.get(&url).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.bytes().await.unwrap().as_ref(),
        b"signed native client payload"
    );
    let tampered = client
        .get(format!("{url}&changed=yes"))
        .send()
        .await
        .unwrap();
    assert_eq!(tampered.status(), StatusCode::FORBIDDEN);
    assert_presigned_credential_ambiguity_rejected(&client, &url, &bound_token).await;
    let legacy = client
        .get(format!("{}/aws-resume.locks/object.bin", server.base))
        .bearer_auth(bound_token)
        .send()
        .await
        .unwrap();
    assert_eq!(legacy.status(), StatusCode::OK);
}

async fn assert_presigned_credential_ambiguity_rejected(client: &Client, url: &str, token: &str) {
    for authorization in [format!("Bearer {token}"), "Bearer invalid-token".to_owned(),
        "AWS4-HMAC-SHA256 Credential=unknown/20261022/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=deadbeef".to_owned()] {
        let response = client.get(url).header("authorization", authorization).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(response.text().await.unwrap().contains("AccessDenied"));
    }
    for extra in [
        "X-Amz-Credential=unknown%2F20261022%2Fus-east-1%2Fs3%2Faws4_request",
        "X-Amz-Credential=SIGNINGKEY%2F20261022%2Fus-east-1%2Fs3%2Faws4_request",
        "%58-Amz-Credential=SIGNINGKEY%2F20261022%2Fus-east-1%2Fs3%2Faws4_request",
        "X-Amz-Signature=deadbeef",
        "X-Amz-Security-Token=invalid-token",
    ] {
        let response = client.get(format!("{url}&{extra}")).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
    let response = client
        .put(url)
        .bearer_auth(token)
        .body("must not replace signed content")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response = client.get(url).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.bytes().await.unwrap().as_ref(),
        b"signed native client payload"
    );
}

#[tokio::test]
async fn s3_copy_part_preserves_source_conditions_and_range_bytes() {
    exercise_copy_part(None).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL DATABASE_URL"]
async fn s3_copy_part_preserves_acknowledged_bytes_on_postgres() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL must be configured");
    exercise_copy_part(Some(&database)).await;
}

async fn exercise_copy_part(database: Option<&str>) {
    let root = tempfile::tempdir().unwrap();
    let server = start(root.path(), ServerFrontend::S3, true, database).await;
    let credential = token("writer", "copy-part", TokenScope::Write);
    let client = Client::new();
    let source = format!("{}/copy-part.locks/source", server.base);
    let response = client
        .put(&source)
        .bearer_auth(&credential)
        .body("0123456789")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let etag = response.headers()["etag"].clone();
    let destination = format!("{}/copy-part.locks/destination", server.base);
    let response = client
        .post(format!("{destination}?uploads"))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap();
    let xml = response.text().await.unwrap();
    let upload = xml
        .split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0;
    let part = format!("{destination}?uploadId={upload}&partNumber=1");
    for (header, date, expected) in [
        (
            "x-amz-copy-source-if-modified-since",
            "Fri, 01 Jan 1960 00:00:00 GMT",
            StatusCode::OK,
        ),
        (
            "x-amz-copy-source-if-modified-since",
            "Fri, 01 Jan 2100 00:00:00 GMT",
            StatusCode::PRECONDITION_FAILED,
        ),
        (
            "x-amz-copy-source-if-unmodified-since",
            "Fri, 01 Jan 1960 00:00:00 GMT",
            StatusCode::PRECONDITION_FAILED,
        ),
        (
            "x-amz-copy-source-if-unmodified-since",
            "Fri, 01 Jan 2100 00:00:00 GMT",
            StatusCode::OK,
        ),
    ] {
        let response = client
            .put(&part)
            .bearer_auth(&credential)
            .header("x-amz-copy-source", "/copy-part.locks/source")
            .header(header, date)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "{header}: {date}");
    }
    for (tag_header, date_header, date, expected) in [
        (
            "x-amz-copy-source-if-match",
            "x-amz-copy-source-if-unmodified-since",
            "Fri, 01 Jan 1960 00:00:00 GMT",
            StatusCode::OK,
        ),
        (
            "x-amz-copy-source-if-none-match",
            "x-amz-copy-source-if-modified-since",
            "Fri, 01 Jan 1960 00:00:00 GMT",
            StatusCode::PRECONDITION_FAILED,
        ),
    ] {
        let response = client
            .put(&part)
            .bearer_auth(&credential)
            .header("x-amz-copy-source", "/copy-part.locks/source")
            .header(tag_header, etag.clone())
            .header(date_header, date)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            expected,
            "{tag_header} with {date_header}"
        );
    }
    for (source, range, condition, expected) in [
        (
            "/other.locks/source",
            "bytes=2-6",
            etag.to_str().unwrap(),
            StatusCode::FORBIDDEN,
        ),
        (
            "/copy-part.locks/source",
            "bytes=2-6",
            "\"wrong\"",
            StatusCode::PRECONDITION_FAILED,
        ),
        (
            "/copy-part.locks/source",
            "bytes=2-99",
            etag.to_str().unwrap(),
            StatusCode::RANGE_NOT_SATISFIABLE,
        ),
        (
            "/copy-part.locks/source",
            "bytes=-3",
            etag.to_str().unwrap(),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let response = client
            .put(&part)
            .bearer_auth(&credential)
            .header("x-amz-copy-source", source)
            .header("x-amz-copy-source-range", range)
            .header("x-amz-copy-source-if-match", condition)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }
    let response = client
        .put(&part)
        .bearer_auth(&credential)
        .header("x-amz-copy-source", "/copy-part.locks/source")
        .header("x-amz-copy-source-range", "bytes=2-6")
        .header("x-amz-copy-source-if-match", etag.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let copied = response.text().await.unwrap();
    assert!(copied.contains("<CopyPartResult"));
    // A later source replacement cannot retroactively change an acknowledged
    // copied part. Rejected attempts to replace that part must preserve it.
    let replaced = client
        .put(&source)
        .bearer_auth(&credential)
        .body("abcdefghij")
        .send()
        .await
        .unwrap();
    assert_eq!(replaced.status(), StatusCode::OK);
    let rejected = client
        .put(&part)
        .bearer_auth(&credential)
        .header("x-amz-copy-source", "/copy-part.locks/source")
        .header("x-amz-copy-source-range", "bytes=2-6")
        .header("x-amz-copy-source-if-match", etag)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::PRECONDITION_FAILED);
    let rejected = client
        .put(&part)
        .bearer_auth(&credential)
        .header("x-amz-copy-source", "/copy-part.locks/source")
        .header("x-amz-copy-source-range", "bytes=2-99")
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    let etag = copied
        .split_once("<ETag>")
        .unwrap()
        .1
        .split_once("</ETag>")
        .unwrap()
        .0;
    server.stop().await;
    let server = start(root.path(), ServerFrontend::S3, true, database).await;
    let destination = format!("{}/copy-part.locks/destination", server.base);
    let response = client.post(format!("{destination}?uploadId={upload}")).bearer_auth(&credential)
        .body(format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"))
        .send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = client
        .get(&destination)
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap().as_ref(), b"23456");
    server.stop().await;
}

#[tokio::test]
#[ignore = "requires the native AWS CLI"]
async fn native_aws_cli_resumes_multipart_after_restart() {
    exercise_native_aws_resume(None, None).await;
}

#[tokio::test]
#[ignore = "requires the native AWS CLI and a disposable PostgreSQL DATABASE_URL"]
async fn native_aws_cli_resumes_multipart_on_postgres_with_shared_storage() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL must be configured");
    exercise_native_aws_resume(Some(&database), None).await;
}

#[tokio::test]
#[ignore = "requires AWS CLI, DATABASE_URL and TEST_S3_* shared object storage"]
async fn native_aws_cli_resumes_multipart_on_postgres_s3_replica() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL must be configured");
    let storage = S3ObjectStoreConfig::new(
        std::env::var("TEST_S3_BUCKET").unwrap(),
        "us-east-1".to_owned(),
    )
    .with_endpoint(Some(std::env::var("TEST_S3_ENDPOINT").unwrap()))
    .with_allow_http(true)
    .with_credentials(
        Some(shardline_protocol::SecretString::new(
            std::env::var("TEST_S3_ACCESS_KEY").unwrap(),
        )),
        Some(shardline_protocol::SecretString::new(
            std::env::var("TEST_S3_SECRET_KEY").unwrap(),
        )),
        None,
    )
    .with_key_prefix(Some(&format!(
        "protocol-resume-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    )));
    exercise_native_aws_resume(Some(&database), Some(storage)).await;
}

async fn exercise_native_aws_resume(database: Option<&str>, storage: Option<S3ObjectStoreConfig>) {
    let root = tempfile::tempdir().unwrap();
    let replica = tempfile::tempdir().unwrap();
    let credential = token("writer", "aws-resume", TokenScope::Write);
    let server = start_with_storage(
        root.path(),
        ServerFrontend::S3,
        true,
        database,
        storage.clone(),
    )
    .await;
    let created = aws_json(&server.base, &credential, &["create-multipart-upload"]).await;
    let upload = created["UploadId"].as_str().unwrap().to_owned();
    let payload = root.path().join("native-part.bin");
    let bytes = b"native AWS multipart recovery bytes";
    std::fs::write(&payload, bytes).unwrap();
    aws_json(
        &server.base,
        &credential,
        &[
            "upload-part",
            "--upload-id",
            &upload,
            "--part-number",
            "1",
            "--body",
            payload.to_str().unwrap(),
        ],
    )
    .await;
    drop(server);
    // PostgreSQL shares session metadata; local object storage must also be
    // shared to resume the bytes. Separate-root discovery is qualified below.
    let resumed_root = if storage.is_some() {
        replica.path()
    } else {
        root.path()
    };
    let server =
        start_with_storage(resumed_root, ServerFrontend::S3, true, database, storage).await;
    let uploads = aws_json(&server.base, &credential, &["list-multipart-uploads"]).await;
    assert!(
        uploads["Uploads"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["UploadId"] == upload)
    );
    let listed = aws_json(
        &server.base,
        &credential,
        &["list-parts", "--upload-id", &upload],
    )
    .await;
    assert_eq!(listed["Parts"][0]["Size"], bytes.len());
    let completion =
        json!({"Parts":[{"PartNumber":1,"ETag":listed["Parts"][0]["ETag"]}]}).to_string();
    aws_json(
        &server.base,
        &credential,
        &[
            "complete-multipart-upload",
            "--upload-id",
            &upload,
            "--multipart-upload",
            &completion,
        ],
    )
    .await;
    let response = Client::new()
        .get(format!("{}/aws-resume.locks/object.bin", server.base))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    let created = aws_json(&server.base, &credential, &["create-multipart-upload"]).await;
    let copy_upload = created["UploadId"].as_str().unwrap();
    let copied = aws_json(
        &server.base,
        &credential,
        &[
            "upload-part-copy",
            "--upload-id",
            copy_upload,
            "--part-number",
            "1",
            "--copy-source",
            "aws-resume.locks/object.bin",
            "--copy-source-range",
            "bytes=7-9",
        ],
    )
    .await;
    let completion =
        json!({"Parts":[{"PartNumber":1,"ETag":copied["CopyPartResult"]["ETag"]}]}).to_string();
    aws_json(
        &server.base,
        &credential,
        &[
            "complete-multipart-upload",
            "--upload-id",
            copy_upload,
            "--multipart-upload",
            &completion,
        ],
    )
    .await;
    let response = Client::new()
        .get(format!("{}/aws-resume.locks/object.bin", server.base))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap().as_ref(), b"AWS");
    let uploads = aws_json(&server.base, &credential, &["list-multipart-uploads"]).await;
    assert!(uploads.get("Uploads").is_none_or(|uploads| {
        uploads
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| entry["UploadId"] != upload)
    }));
}

async fn aws_json(endpoint: &str, credential: &str, arguments: &[&str]) -> Value {
    let mut command = std::process::Command::new("aws");
    command
        .args([
            "--endpoint-url",
            endpoint,
            "--region",
            "us-east-1",
            "--output",
            "json",
            "s3api",
        ])
        .args(arguments)
        .args(["--bucket", "aws-resume.locks"])
        .env("AWS_ACCESS_KEY_ID", credential)
        .env("AWS_SECRET_ACCESS_KEY", "legacy-token-bridge")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env("AWS_PAGER", "")
        .env_remove("AWS_SESSION_TOKEN");
    if arguments[0] != "list-multipart-uploads" {
        command.args(["--key", "object.bin"]);
    }
    let output = tokio::task::spawn_blocking(move || command.output())
        .await
        .unwrap()
        .expect("AWS CLI must be installed");
    assert!(
        output.status.success(),
        "AWS CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[tokio::test]
async fn s3_upload_discovery_filters_groups_and_paginates() {
    exercise_upload_discovery(None).await;
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL DATABASE_URL"]
async fn s3_upload_discovery_on_postgres_replicas() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL must be configured");
    exercise_upload_discovery(Some(&database)).await;
}

async fn exercise_upload_discovery(database: Option<&str>) {
    let root = tempfile::tempdir().unwrap();
    let replica_root = tempfile::tempdir().unwrap();
    let repository = format!(
        "discovery-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    );
    let credential = token("writer", &repository, TokenScope::Write);
    let server = start(root.path(), ServerFrontend::S3, true, database).await;
    let client = Client::new();
    let bucket = format!("{repository}.locks");
    let mut uploads = Vec::new();
    for key in ["a", "folder/one", "folder/two", "z", "a"] {
        let response = client
            .post(format!("{}/{bucket}/{key}?uploads", server.base))
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let xml = response.text().await.unwrap();
        let id = xml
            .split_once("<UploadId>")
            .unwrap()
            .1
            .split_once("</UploadId>")
            .unwrap()
            .0
            .to_owned();
        uploads.push((key, id));
    }
    drop(server);
    let restarted_root = if database.is_some() {
        replica_root.path()
    } else {
        root.path()
    };
    let server = start(restarted_root, ServerFrontend::S3, true, database).await;
    let base = format!("{}/{bucket}?uploads", server.base);
    let response = client
        .get(format!("{base}&max-uploads=1"))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let first = response.text().await.unwrap();
    assert!(first.contains("<Key>a</Key>"));
    assert!(first.contains("<IsTruncated>true</IsTruncated>"));
    let marker = first
        .split_once("<NextUploadIdMarker>")
        .unwrap()
        .1
        .split_once("</NextUploadIdMarker>")
        .unwrap()
        .0;
    let response = client
        .get(format!(
            "{base}&max-uploads=1&key-marker=a&upload-id-marker={marker}"
        ))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let second = response.text().await.unwrap();
    assert!(second.contains("<Key>a</Key>"));
    assert!(!second.contains(&format!("<UploadId>{marker}</UploadId>")));
    let grouped = client
        .get(format!("{base}&key-marker=a&delimiter=/&max-uploads=1"))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(grouped.contains("<CommonPrefixes><Prefix>folder/</Prefix></CommonPrefixes>"));
    assert!(grouped.contains("<IsTruncated>true</IsTruncated>"));
    let next = client
        .get(format!("{base}&key-marker=folder/&delimiter=/"))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(next.contains("<Key>z</Key>"));
    assert!(!next.contains("<CommonPrefixes>"));
    let encoded = client
        .get(format!("{base}&prefix=folder/&encoding-type=url"))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(encoded.contains("<Key>folder%2Fone</Key>"));
    assert!(!encoded.contains("<Key>a</Key>"));
    // Manual markers need not name an existing upload. Without a key marker,
    // the upload-ID marker has no effect; a key-only marker excludes that key.
    for (query, expected_a_count) in [
        ("upload-id-marker=zzzz", 2),
        ("key-marker=a", 0),
        ("key-marker=a&upload-id-marker=!", 2),
        ("key-marker=a&upload-id-marker=zzzz", 0),
        ("key-marker=b&upload-id-marker=!", 0),
    ] {
        let response = client
            .get(format!("{base}&{query}"))
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{query}");
        let xml = response.text().await.unwrap();
        assert_eq!(
            xml.matches("<Key>a</Key>").count(),
            expected_a_count,
            "{query}: {xml}"
        );
        assert!(xml.contains("<Key>folder/one</Key>"), "{query}: {xml}");
        assert!(xml.contains("<Key>z</Key>"), "{query}: {xml}");
    }
    for invalid in [
        "max-uploads=0",
        "max-uploads=1001",
        "prefix=a&prefix=b",
        "encoding-type=bad",
        "key-marker=a&key-marker=b",
        "upload-id-marker=a&upload-id-marker=b",
    ] {
        assert_eq!(
            client
                .get(format!("{base}&{invalid}"))
                .bearer_auth(&credential)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        client
            .get(&base)
            .bearer_auth(token("other", "other", TokenScope::Read))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    for (key, id) in uploads {
        assert_eq!(
            client
                .delete(format!("{}/{bucket}/{key}?uploadId={id}", server.base))
                .bearer_auth(&credential)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
    }
    let empty = client
        .get(&base)
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!empty.contains("<Upload>"));
}

#[tokio::test]
async fn s3_multipart_parts_resume_after_restart_and_paginate() {
    exercise_s3_parts(None).await;
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL DATABASE_URL"]
async fn s3_multipart_parts_resume_on_postgres() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL must be configured");
    exercise_s3_parts(Some(&database)).await;
}

async fn exercise_s3_parts(database: Option<&str>) {
    let root = tempfile::tempdir().unwrap();
    let server = start(root.path(), ServerFrontend::S3, true, database).await;
    let client = Client::new();
    let credential = token("writer", "resume", TokenScope::Write);
    let path = "/resume.locks/object.bin";
    let response = client
        .post(format!("{}{path}?uploads", server.base))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let xml = response.text().await.unwrap();
    let upload = xml
        .split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0
        .to_owned();
    for number in [1, 3] {
        let response = client
            .put(format!(
                "{}{path}?uploadId={upload}&partNumber={number}",
                server.base
            ))
            .bearer_auth(&credential)
            .body(format!("part-{number}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    drop(server);
    let server = start(root.path(), ServerFrontend::S3, true, database).await;
    let url = format!("{}{path}?uploadId={upload}", server.base);
    let response = client
        .get(format!("{url}&max-parts=1"))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let xml = response.text().await.unwrap();
    assert!(xml.contains("<PartNumber>1</PartNumber>"));
    assert!(xml.contains("<NextPartNumberMarker>1</NextPartNumberMarker>"));
    assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
    let response = client
        .get(format!("{url}&part-number-marker=1&max-parts=1"))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let xml = response.text().await.unwrap();
    assert!(xml.contains("<PartNumber>3</PartNumber>"));
    assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
    for invalid in ["max-parts=1001", "max-parts=-1", "max-parts=1&max-parts=2"] {
        assert_eq!(
            client
                .get(format!("{url}&{invalid}"))
                .bearer_auth(&credential)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    let wrong_key = client
        .get(format!(
            "{}/resume.locks/wrong?uploadId={upload}",
            server.base
        ))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_key.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        client
            .delete(&url)
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        client
            .get(&url)
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn hub_lfs_lock_urls_enforce_repository_binding_and_unlock() {
    let root = tempfile::tempdir().unwrap();
    let server = start(root.path(), ServerFrontend::Hub, true, None).await;
    let client = Client::new();
    let alice = token("alice", "hub-owner", TokenScope::Write);
    let base = format!("{}/hub-owner/locks.git/info/lfs/locks", server.base);
    let created = client
        .post(&base)
        .bearer_auth(&alice)
        .json(&json!({"path":"weights.bin"}))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let created: Value = created.json().await.unwrap();
    let id = created["lock"]["id"].as_str().unwrap();
    let typed = format!(
        "{}/datasets/hub-owner/locks.git/info/lfs/locks",
        server.base
    );
    let listed: Value = client
        .get(&typed)
        .bearer_auth(&alice)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["locks"][0]["id"], id);
    let denied = client
        .get(format!("{}/other/locks.git/info/lfs/locks", server.base))
        .bearer_auth(&alice)
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    let unlocked = client
        .post(format!("{typed}/{id}/unlock"))
        .bearer_auth(&alice)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(unlocked.status(), StatusCode::OK);
}

#[tokio::test]
async fn hub_lfs_lock_force_unlock_and_pagination_follow_protocol_contract() {
    let root = tempfile::tempdir().unwrap();
    let server = start(root.path(), ServerFrontend::Hub, true, None).await;
    let client = Client::new();
    let alice = token("alice", "hub-lock-pages", TokenScope::Write);
    let bob = token("bob", "hub-lock-pages", TokenScope::Write);
    let base = format!("{}/hub-lock-pages/locks.git/info/lfs/locks", server.base);

    let mut alice_lock_id = None;
    for path in ["a.bin", "b.bin", "c.bin"] {
        let created = client
            .post(&base)
            .bearer_auth(&alice)
            .json(&json!({"path":path}))
            .send()
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        let body: Value = created.json().await.unwrap();
        let id = body["lock"]["id"].as_str().unwrap().to_owned();
        alice_lock_id.get_or_insert(id);
    }
    let bob_created = client
        .post(&base)
        .bearer_auth(&bob)
        .json(&json!({"path":"d.bin"}))
        .send()
        .await
        .unwrap();
    assert_eq!(bob_created.status(), StatusCode::CREATED);

    // The cursor is an opaque keyset position. Following it must return every
    // lock exactly once even though lock identifiers are generated randomly.
    let mut cursor = None;
    let mut ids = Vec::new();
    loop {
        let mut request = client
            .get(&base)
            .bearer_auth(&alice)
            .query(&[("limit", "2")]);
        if let Some(cursor) = cursor.as_deref() {
            request = request.query(&[("cursor", cursor)]);
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = response.json().await.unwrap();
        let locks = body["locks"].as_array().unwrap();
        assert!(locks.len() <= 2);
        ids.extend(
            locks
                .iter()
                .map(|lock| lock["id"].as_str().unwrap().to_owned()),
        );
        cursor = body["next_cursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 4);

    let mut cursor = None;
    let mut verified_ids = Vec::new();
    loop {
        let body = cursor.as_ref().map_or_else(
            || json!({"limit":2}),
            |cursor| json!({"limit":2,"cursor":cursor}),
        );
        let response = client
            .post(format!("{base}/verify"))
            .bearer_auth(&alice)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = response.json().await.unwrap();
        for category in ["ours", "theirs"] {
            verified_ids.extend(
                body[category]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|lock| lock["id"].as_str().unwrap().to_owned()),
            );
        }
        cursor = body["next_cursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    verified_ids.sort();
    verified_ids.dedup();
    assert_eq!(verified_ids, ids);

    let lock_id = alice_lock_id.unwrap();
    let unlock_url = format!("{base}/{lock_id}/unlock");
    let denied = client
        .post(&unlock_url)
        .bearer_auth(&bob)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);

    let forced = client
        .post(&unlock_url)
        .bearer_auth(&bob)
        .json(&json!({"force":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(forced.status(), StatusCode::OK);
    let forced: Value = forced.json().await.unwrap();
    assert_eq!(forced["lock"]["id"], lock_id);

    let gone = client
        .get(&base)
        .bearer_auth(&alice)
        .query(&[("id", lock_id.as_str())])
        .send()
        .await
        .unwrap();
    assert_eq!(gone.status(), StatusCode::OK);
    let gone: Value = gone.json().await.unwrap();
    assert_eq!(gone["locks"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn anonymous_hub_locks_are_isolated_by_repository() {
    let root = tempfile::tempdir().unwrap();
    let server = start(root.path(), ServerFrontend::Hub, false, None).await;
    let client = Client::new();
    for owner in ["first", "second"] {
        let response = client
            .post(format!("{}/{owner}/repo.git/info/lfs/locks", server.base))
            .json(&json!({"path":"same.bin"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }
}

#[tokio::test]
async fn hub_lfs_upload_url_checks_both_credentials() {
    let root = tempfile::tempdir().unwrap();
    let server = start(root.path(), ServerFrontend::Hub, true, None).await;
    let client = Client::new();
    let writer = token("alice", "dual-auth", TokenScope::Write);
    let bytes = b"object-scoped upload fixture";
    let oid = hex::encode(Sha256::digest(bytes));
    let batch = client.post(format!("{}/dual-auth/locks.git/info/lfs/objects/batch", server.base))
        .bearer_auth(&writer).json(&json!({"operation":"upload", "transfers":["basic"], "objects":[{"oid":oid,"size":bytes.len()}]}))
        .send().await.unwrap();
    assert_eq!(batch.status(), StatusCode::OK);
    let batch: Value = batch.json().await.unwrap();
    let url = batch["objects"][0]["actions"]["upload"]["href"]
        .as_str()
        .unwrap();
    for credential in [
        token("alice", "different", TokenScope::Write),
        token("alice", "dual-auth", TokenScope::Read),
        "invalid".to_owned(),
    ] {
        let denied = client
            .put(url)
            .bearer_auth(credential)
            .body(bytes.to_vec())
            .send()
            .await
            .unwrap();
        assert!(denied.status().is_client_error());
    }
    let mut conflicting = reqwest::header::HeaderMap::new();
    conflicting.append(
        reqwest::header::AUTHORIZATION,
        format!("Bearer {writer}").parse().unwrap(),
    );
    conflicting.append(
        reqwest::header::AUTHORIZATION,
        "Bearer invalid".parse().unwrap(),
    );
    assert!(
        client
            .put(url)
            .headers(conflicting)
            .body(bytes.to_vec())
            .send()
            .await
            .unwrap()
            .status()
            .is_client_error()
    );
    let mut identical = reqwest::header::HeaderMap::new();
    for _ in 0..2 {
        identical.append(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {writer}").parse().unwrap(),
        );
    }
    let accepted = client
        .put(url)
        .headers(identical)
        .body(bytes.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::OK);
}

struct Runtime {
    base: String,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    background: Option<app::RouterBackgroundTasks>,
}
impl Drop for Runtime {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Runtime {
    async fn stop(mut self) {
        self.task.abort();
        let _stopped = (&mut self.task).await;
        if let Some(background) = self.background.take() {
            background.shutdown().await.unwrap();
        }
    }
}

async fn start(
    root: &Path,
    frontend: ServerFrontend,
    auth: bool,
    database: Option<&str>,
) -> Runtime {
    start_with_storage(root, frontend, auth, database, None).await
}

async fn start_with_storage(
    root: &Path,
    frontend: ServerFrontend,
    auth: bool,
    database: Option<&str>,
    storage: Option<S3ObjectStoreConfig>,
) -> Runtime {
    start_configured(root, frontend, auth, database, storage, None).await
}

async fn start_configured(
    root: &Path,
    frontend: ServerFrontend,
    auth: bool,
    database: Option<&str>,
    storage: Option<S3ObjectStoreConfig>,
    signing: Option<shardline_server::S3SigningIdentity>,
) -> Runtime {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let mut config = ServerConfig::new(
        listener.local_addr().unwrap(),
        base.clone(),
        root.to_owned(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_server_frontends(vec![frontend])
    .unwrap()
    .with_reconstruction_cache_disabled();
    if let Some(storage) = storage {
        config = config.with_object_storage(ObjectStorageAdapter::S3, Some(storage));
    }
    if frontend == ServerFrontend::Hub {
        config = config.with_hub_webhook_secret_key(vec![7; 32]).unwrap();
    }
    if auth {
        config = config.with_token_signing_key(KEY.to_vec()).unwrap();
    }
    if let Some(signing) = signing {
        config = config.with_s3_signing_identity(signing).unwrap();
    }
    if let Some(database) = database {
        let pool = sqlx::PgPool::connect(database).await.unwrap();
        shardline_server::apply_database_migrations(&pool)
            .await
            .unwrap();
        pool.close().await;
        config = config.with_index_postgres_url(database.to_owned()).unwrap();
    }
    let (router, background) = app::router_with_background_tasks(config).await.unwrap();
    Runtime {
        base,
        task: tokio::spawn(async move { axum::serve(listener, router).await }),
        background: Some(background),
    }
}

fn token(subject: &str, repository: &str, scope: TokenScope) -> String {
    revision_token(subject, repository, scope, None)
}

fn revision_token(
    subject: &str,
    repository: &str,
    scope: TokenScope,
    revision: Option<&str>,
) -> String {
    let repository =
        RepositoryScope::new(RepositoryProvider::Generic, repository, "locks", revision).unwrap();
    let claims = TokenClaims::new("completion", subject, scope, repository, u64::MAX).unwrap();
    LocalHmacProvider::new(KEY)
        .unwrap()
        .mint_token(&claims)
        .unwrap()
}

fn lock_url(base: &str, repository: &str, frontend: ServerFrontend) -> String {
    if frontend == ServerFrontend::Hub {
        format!("{base}/{repository}/locks.git/info/lfs/locks")
    } else {
        format!("{base}/v1/lfs/locks")
    }
}

async fn exercise_locks(database: Option<&str>, frontend: ServerFrontend) {
    let root = tempfile::tempdir().unwrap();
    let repository = format!(
        "locks-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    );
    let alice = token("alice", &repository, TokenScope::Write);
    let bob = token("bob", &repository, TokenScope::Write);
    let other = token("alice", &format!("{repository}-other"), TokenScope::Write);
    let read = token("alice", &repository, TokenScope::Read);
    let client = Client::new();
    let server = start(root.path(), frontend, true, database).await;
    let mut url = lock_url(&server.base, &repository, frontend);
    let created = client
        .post(&url)
        .bearer_auth(&alice)
        .json(&json!({"path":"models/weights.bin"}))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    assert_eq!(
        created.headers()["content-type"],
        "application/vnd.git-lfs+json"
    );
    let created: Value = created.json().await.unwrap();
    let id = created["lock"]["id"].as_str().unwrap();
    assert_eq!(created["lock"]["owner"]["name"], "alice");
    let revision = revision_token("alice", &repository, TokenScope::Write, Some("main"));
    for (endpoint, body) in [
        (
            url.clone(),
            json!({"path":"forbidden.bin", "ref":{"name":"refs/heads/other"}}),
        ),
        (
            format!("{url}/verify"),
            json!({"ref":{"name":"refs/heads/other"}}),
        ),
        (
            format!("{url}/{id}/unlock"),
            json!({"force":true, "ref":{"name":"refs/heads/other"}}),
        ),
    ] {
        let denied = client
            .post(endpoint)
            .bearer_auth(&revision)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    }
    let denied = client
        .get(&url)
        .bearer_auth(&revision)
        .query(&[("refspec", "refs/heads/other")])
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    let verified: Value = client
        .post(format!("{url}/verify"))
        .bearer_auth(&revision)
        .json(&json!({"ref":{"name":"refs/heads/main"}}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(verified["ours"][0]["id"], id);
    let duplicate = client
        .post(&url)
        .bearer_auth(&bob)
        .json(&json!({"path":"models/weights.bin"}))
        .send()
        .await
        .unwrap();
    assert_eq!(duplicate.status(), StatusCode::CONFLICT);
    assert_eq!(duplicate.json::<Value>().await.unwrap()["lock"]["id"], id);
    assert_eq!(
        client
            .post(&url)
            .bearer_auth(&read)
            .json(&json!({"path":"readonly"}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .post(&url)
            .bearer_auth(&alice)
            .json(&json!({"path":"../escape"}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let verify: Value = client
        .post(format!("{url}/verify"))
        .bearer_auth(&bob)
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(verify["ours"].as_array().unwrap().is_empty());
    assert_eq!(verify["theirs"][0]["id"], id);
    let unlock = format!("{url}/{id}/unlock");
    assert_eq!(
        client
            .post(&unlock)
            .bearer_auth(&bob)
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .post(&unlock)
            .bearer_auth(&other)
            .json(&json!({"force":true}))
            .send()
            .await
            .unwrap()
            .status(),
        if frontend == ServerFrontend::Hub {
            // Hub binds the URL repository before looking up a lock ID.
            StatusCode::FORBIDDEN
        } else {
            StatusCode::NOT_FOUND
        }
    );
    server.stop().await;
    let restarted = start(root.path(), frontend, true, database).await;
    url = lock_url(&restarted.base, &repository, frontend);
    let listed: Value = client
        .get(&url)
        .bearer_auth(&alice)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["locks"][0], created["lock"]);
    assert_eq!(
        client
            .post(format!("{url}/{id}/unlock"))
            .bearer_auth(&bob)
            .json(&json!({"force":true}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let mut tasks = tokio::task::JoinSet::new();
    let other_root = tempfile::tempdir().unwrap();
    let other_server = start(
        if database.is_some() {
            other_root.path()
        } else {
            root.path()
        },
        frontend,
        true,
        database,
    )
    .await;
    let other_url = lock_url(&other_server.base, &repository, frontend);
    for index in 0..16 {
        let client = client.clone();
        let alice = alice.clone();
        let url = if index % 2 == 0 {
            url.clone()
        } else {
            other_url.clone()
        };
        tasks.spawn(async move {
            client
                .post(url)
                .bearer_auth(alice)
                .json(&json!({"path":"race.bin"}))
                .send()
                .await
                .unwrap()
                .status()
        });
    }
    let mut winners = 0_u32;
    while let Some(result) = tasks.join_next().await {
        let status = result.unwrap();
        if status == StatusCode::CREATED {
            winners = winners.saturating_add(1);
        } else {
            assert_eq!(status, StatusCode::CONFLICT);
        }
    }
    assert_eq!(winners, 1);
    for path in ["page/a", "page/b"] {
        assert_eq!(
            client
                .post(&url)
                .bearer_auth(&alice)
                .json(&json!({"path":path}))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
    }
    let mut cursor: Option<String> = None;
    let mut ids = std::collections::BTreeSet::new();
    loop {
        let mut request = client
            .get(&url)
            .bearer_auth(&alice)
            .query(&[("limit", "1")]);
        if let Some(cursor) = &cursor {
            request = request.query(&[("cursor", cursor)]);
        }
        let page: Value = request.send().await.unwrap().json().await.unwrap();
        for lock in page["locks"].as_array().unwrap() {
            assert!(ids.insert(lock["id"].as_str().unwrap().to_owned()));
        }
        cursor = page
            .get("next_cursor")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(ids.len(), 3);
    other_server.stop().await;
    restarted.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lfs_locks_are_exclusive_owned_scoped_and_durable_on_sqlite() {
    exercise_locks(None, ServerFrontend::Lfs).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn lfs_locks_are_exclusive_owned_scoped_and_durable_on_postgres() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
    exercise_locks(Some(&database), ServerFrontend::Lfs).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hub_lfs_locks_are_exclusive_owned_scoped_and_durable_on_sqlite() {
    exercise_locks(None, ServerFrontend::Hub).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn hub_lfs_locks_are_exclusive_owned_scoped_and_durable_on_postgres() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
    exercise_locks(Some(&database), ServerFrontend::Hub).await;
}

async fn publish(client: &Client, base: &str, repository: &str, document: &Value) -> String {
    let bytes = serde_json::to_vec(document).unwrap();
    let digest = format!("sha256:{}", hex::encode(Sha256::digest(&bytes)));
    let response = client
        .put(format!("{base}/v2/{repository}/manifests/{digest}"))
        .header("content-type", INDEX)
        .body(bytes)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        response.text().await.unwrap()
    );
    if let Some(subject) = document.get("subject") {
        assert_eq!(headers["OCI-Subject"], subject["digest"].as_str().unwrap());
    }
    digest
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oci_referrers_filter_paginate_survive_restart_and_respect_deletion() {
    exercise_referrers(None, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn oci_referrers_on_postgres() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
    exercise_referrers(Some(&database), None).await;
}

async fn exercise_referrers(database: Option<&str>, storage: Option<S3ObjectStoreConfig>) {
    let root = tempfile::tempdir().unwrap();
    let replica_root = tempfile::tempdir().unwrap();
    let recovery_root = tempfile::tempdir().unwrap();
    let republished_root = tempfile::tempdir().unwrap();
    let server = start_with_storage(
        root.path(),
        ServerFrontend::Oci,
        false,
        database,
        storage.clone(),
    )
    .await;
    let peer = if storage.is_some() {
        Some(
            start_with_storage(
                replica_root.path(),
                ServerFrontend::Oci,
                false,
                database,
                storage.clone(),
            )
            .await,
        )
    } else {
        None
    };
    let repository = format!(
        "completion-{}/artifacts",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    );
    let client = Client::new();
    let subject = format!("sha256:{}", "a".repeat(64));
    let mut digests = Vec::new();
    let mut documents = Vec::new();
    for artifact_type in ["application/example.signature", "application/example.sbom"] {
        let document = json!({"schemaVersion":2,"mediaType":INDEX,"manifests":[],"artifactType":artifact_type,"annotations":{"org.example.kind":artifact_type},"subject":{"mediaType":INDEX,"digest":subject,"size":123}});
        let digest = if let Some(peer) = &peer {
            let (first, second) = tokio::join!(
                publish(&client, &server.base, &repository, &document),
                publish(&client, &peer.base, &repository, &document)
            );
            assert_eq!(
                first, second,
                "concurrent replicas must acknowledge the same immutable digest"
            );
            first
        } else {
            publish(&client, &server.base, &repository, &document).await
        };
        digests.push(digest);
        documents.push(document);
    }
    publish(
        &client,
        &server.base,
        &repository,
        &json!({"schemaVersion":2,"mediaType":INDEX,"manifests":[]}),
    )
    .await;
    let mut unrelated = documents[0].clone();
    unrelated["subject"]["digest"] = json!(format!("sha256:{}", "b".repeat(64)));
    publish(&client, &server.base, &repository, &unrelated).await;
    // A missing row models a legacy or mixed-version writer. Discovery must
    // reconcile immutable content instead of treating absence as no referrer.
    oci_index::evict(root.path(), database, &repository, &digests[0]).await;
    // A derived mapping can outlive reclaimed physical content. It must not
    // make a missing manifest appear visible or break the entire listing.
    oci_index::orphan(root.path(), database, &repository, &subject).await;
    let listing_base = peer.as_ref().map_or(&server.base, |peer| &peer.base);
    let mut url = format!("{listing_base}/v2/{repository}/referrers/{subject}");
    let first = client.get(&url).query(&[("n", "1")]).send().await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.headers()["content-type"], INDEX);
    let link = first.headers()["link"].to_str().unwrap().to_owned();
    let first: Value = first.json().await.unwrap();
    assert_eq!(first["manifests"].as_array().unwrap().len(), 1);
    let next = link.split_once('<').unwrap().1.split_once('>').unwrap().0;
    let second: Value = client
        .get(format!("{}{next}", server.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_ne!(
        first["manifests"][0]["digest"],
        second["manifests"][0]["digest"]
    );
    let filtered = client
        .get(&url)
        .query(&[("artifactType", "application/example.signature")])
        .send()
        .await
        .unwrap();
    assert_eq!(
        filtered.status(),
        StatusCode::OK,
        "filtered referrers must tolerate an orphaned derived mapping"
    );
    assert_eq!(filtered.headers()["OCI-Filters-Applied"], "artifactType");
    let filtered: Value = filtered.json().await.unwrap();
    assert_eq!(filtered["manifests"].as_array().unwrap().len(), 1);
    let other: Value = client
        .get(format!(
            "{}/v2/other/artifacts/referrers/{subject}",
            server.base
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(other["manifests"].as_array().unwrap().is_empty());
    server.stop().await;
    if let Some(peer) = peer {
        peer.stop().await;
    }
    let restart_root = if storage.is_some() {
        recovery_root.path()
    } else {
        root.path()
    };
    let restarted = start_with_storage(
        restart_root,
        ServerFrontend::Oci,
        false,
        database,
        storage.clone(),
    )
    .await;
    oci_index::evict(root.path(), database, &repository, &digests[1]).await;
    url = format!("{}/v2/{repository}/referrers/{subject}", restarted.base);
    let listed: Value = client.get(&url).send().await.unwrap().json().await.unwrap();
    assert_eq!(listed["manifests"].as_array().unwrap().len(), 2);
    for digest in &digests {
        assert_eq!(
            client
                .delete(format!(
                    "{}/v2/{repository}/manifests/{digest}",
                    restarted.base
                ))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::ACCEPTED
        );
    }
    let response = client.get(&url).send().await.unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "referrers after deletion: {body}");
    let after_delete: Value = serde_json::from_str(&body).unwrap();
    assert!(after_delete["manifests"].as_array().unwrap().is_empty());

    // Republish exactly the same immutable bytes. A derived referrer index
    // must not treat an old deletion as permanent or append duplicate entries
    // when a client retries an already acknowledged publication.
    for (document, digest) in documents.iter().zip(&digests) {
        for _ in 0..2 {
            assert_eq!(
                publish(&client, &restarted.base, &repository, document).await,
                *digest
            );
        }
    }
    restarted.stop().await;
    let final_root = if storage.is_some() {
        republished_root.path()
    } else {
        root.path()
    };
    let republished =
        start_with_storage(final_root, ServerFrontend::Oci, false, database, storage).await;
    let url = format!("{}/v2/{repository}/referrers/{subject}", republished.base);
    let response = client.get(&url).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let restored: Value = response.json().await.unwrap();
    let mut actual: Vec<_> = restored["manifests"]
        .as_array()
        .unwrap()
        .iter()
        .map(|descriptor| descriptor["digest"].as_str().unwrap().to_owned())
        .collect();
    actual.sort();
    digests.sort();
    assert_eq!(
        actual, digests,
        "republication must restore each digest once"
    );
    for document in &documents {
        let artifact_type = document["artifactType"].as_str().unwrap();
        let response = client
            .get(&url)
            .query(&[("artifactType", artifact_type), ("n", "1")])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("link").is_none());
        let filtered: Value = response.json().await.unwrap();
        assert_eq!(filtered["manifests"].as_array().unwrap().len(), 1);
        assert_eq!(filtered["manifests"][0]["artifactType"], artifact_type);
    }
    let previous =
        oci_index::replace_commitment(root.path(), database, &repository, &digests[0], "corrupt")
            .await;
    let response = client.get(&url).send().await.unwrap();
    assert!(
        response.status().is_server_error(),
        "corrupt index must not return a partial successful inventory"
    );
    let unchanged = client
        .get(format!(
            "{}/v2/{repository}/manifests/{}",
            republished.base, digests[0]
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(unchanged.status(), StatusCode::OK);
    assert_eq!(
        format!(
            "sha256:{}",
            hex::encode(Sha256::digest(unchanged.bytes().await.unwrap()))
        ),
        digests[0],
        "lookup metadata must not change domain content"
    );
    oci_index::replace_commitment(root.path(), database, &repository, &digests[0], &previous).await;
    assert_eq!(
        client.get(&url).send().await.unwrap().status(),
        StatusCode::OK
    );
    republished.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires OCI_CONFORMANCE_BIN from distribution-spec v1.1.1"]
async fn official_oci_distribution_conformance() {
    let executable = std::env::var("OCI_CONFORMANCE_BIN").expect("OCI_CONFORMANCE_BIN is required");
    let root = tempfile::tempdir().unwrap();
    let server = start(root.path(), ServerFrontend::Oci, false, None).await;
    let mut command = std::process::Command::new(executable);
    command
        .current_dir(root.path())
        .arg("-ginkgo.timeout=5m")
        .arg("-ginkgo.no-color")
        .arg("-ginkgo.v")
        .env("OCI_ROOT_URL", &server.base)
        .env("OCI_NAMESPACE", "conformance/main")
        .env("OCI_CROSSMOUNT_NAMESPACE", "conformance/other")
        .env("OCI_TEST_PULL", "1")
        .env("OCI_TEST_PUSH", "1")
        .env("OCI_TEST_CONTENT_DISCOVERY", "1")
        .env("OCI_TEST_CONTENT_MANAGEMENT", "1")
        .env("OCI_DELETE_MANIFEST_BEFORE_BLOBS", "1")
        .env("OCI_AUTOMATIC_CROSSMOUNT", "1")
        .env("OCI_REPORT_DIR", "none");
    let output = tokio::task::spawn_blocking(move || command.output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "official OCI suite failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!("{}", String::from_utf8_lossy(&output.stdout));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oci_errors_distinguish_missing_blobs_manifests_and_dependencies() {
    let root = tempfile::tempdir().unwrap();
    let server = start(root.path(), ServerFrontend::Oci, false, None).await;
    let client = Client::new();
    let missing = format!("sha256:{}", "b".repeat(64));
    for (path, code) in [
        (format!("blobs/{missing}"), "BLOB_UNKNOWN"),
        (
            "manifests/.INVALID_MANIFEST_NAME".to_owned(),
            "MANIFEST_UNKNOWN",
        ),
    ] {
        let response = client
            .get(format!("{}/v2/errors/test/{path}", server.base))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let error: Value = response.json().await.unwrap();
        assert_eq!(error["errors"][0]["code"], code);
    }
    for (document, code) in [
        (
            json!({"schemaVersion":2,"mediaType":INDEX,"manifests":[{"mediaType":INDEX,"digest":missing,"size":1}]}),
            "MANIFEST_BLOB_UNKNOWN",
        ),
        (
            json!({"schemaVersion":1,"mediaType":INDEX,"manifests":[]}),
            "MANIFEST_INVALID",
        ),
    ] {
        let response = client
            .put(format!("{}/v2/errors/test/manifests/latest", server.base))
            .header("content-type", INDEX)
            .json(&document)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error: Value = response.json().await.unwrap();
        assert_eq!(error["errors"][0]["code"], code);
    }
    let upload = client
        .post(format!("{}/v2/errors/test/blobs/uploads/", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::ACCEPTED);
    let location = upload.headers()["location"].to_str().unwrap();
    let session_url = reqwest::Url::parse(&server.base)
        .unwrap()
        .join(location)
        .unwrap();
    assert_eq!(
        client
            .delete(session_url.clone())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    let missing_session = client.get(session_url).send().await.unwrap();
    assert_eq!(missing_session.status(), StatusCode::NOT_FOUND);
    let error: Value = missing_session.json().await.unwrap();
    assert_eq!(error["errors"][0]["code"], "BLOB_UPLOAD_UNKNOWN");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires native git-lfs"]
async fn native_git_lfs_lock_list_verify_and_unlock() {
    exercise_native_git_lfs(ServerFrontend::Lfs, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires native git-lfs"]
async fn native_hub_git_lfs_lock_and_transfer() {
    exercise_native_git_lfs(ServerFrontend::Hub, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires native git-lfs and disposable PostgreSQL via DATABASE_URL"]
async fn native_git_lfs_on_postgres() {
    let database = std::env::var("DATABASE_URL").unwrap();
    exercise_native_git_lfs(ServerFrontend::Lfs, Some(&database)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires native git-lfs and disposable PostgreSQL via DATABASE_URL"]
async fn native_hub_git_lfs_on_postgres() {
    let database = std::env::var("DATABASE_URL").unwrap();
    exercise_native_git_lfs(ServerFrontend::Hub, Some(&database)).await;
}

async fn exercise_native_git_lfs(frontend: ServerFrontend, database: Option<&str>) {
    let root = tempfile::tempdir().unwrap();
    let server = start(root.path(), frontend, true, database).await;
    let worktree = tempfile::tempdir().unwrap();
    let repository = format!(
        "native-lfs-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    );
    let credential = revision_token(
        "native-client",
        &repository,
        TokenScope::Write,
        Some("main"),
    );
    async fn git(directory: &Path, args: &[&str]) -> Vec<u8> {
        let directory = directory.to_owned();
        let args: Vec<_> = args.iter().map(|arg| (*arg).to_owned()).collect();
        let output = tokio::task::spawn_blocking(move || {
            std::process::Command::new("git")
                .arg("-C")
                .arg(directory)
                .args(args)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .output()
        })
        .await
        .unwrap()
        .expect("git and git-lfs must be installed");
        assert!(
            output.status.success(),
            "native git command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }
    git(worktree.path(), &["init"]).await;
    git(worktree.path(), &["branch", "-M", "main"]).await;
    std::fs::write(worktree.path().join("model.bin"), b"native locking fixture").unwrap();
    git(worktree.path(), &["add", "model.bin"]).await;
    git(
        worktree.path(),
        &[
            "-c",
            "user.name=Protocol Test",
            "-c",
            "user.email=protocol@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "locking fixture",
        ],
    )
    .await;
    git(
        worktree.path(),
        &[
            "remote",
            "add",
            "origin",
            "https://example.invalid/test/repository.git",
        ],
    )
    .await;
    let lfs_path = if frontend == ServerFrontend::Hub {
        format!("/{repository}/locks.git/info/lfs")
    } else {
        "/v1/lfs".to_owned()
    };
    git(
        worktree.path(),
        &["config", "lfs.url", &format!("{}{lfs_path}", server.base)],
    )
    .await;
    git(
        worktree.path(),
        &[
            "config",
            "http.extraHeader",
            &format!("Authorization: Bearer {credential}"),
        ],
    )
    .await;
    git(worktree.path(), &["config", "lfs.locksverify", "true"]).await;
    let locked: Value = serde_json::from_slice(
        &git(worktree.path(), &["lfs", "lock", "model.bin", "--json"]).await,
    )
    .unwrap();
    let locked = &locked[0];
    assert_eq!(locked["path"], "model.bin");
    let listed: Value =
        serde_json::from_slice(&git(worktree.path(), &["lfs", "locks", "--json"]).await).unwrap();
    assert_eq!(listed[0]["id"], locked["id"]);
    git(worktree.path(), &["lfs", "locks", "--verify", "--json"]).await;
    git(worktree.path(), &["lfs", "unlock", "model.bin", "--json"]).await;
    let after_unlock: Value =
        serde_json::from_slice(&git(worktree.path(), &["lfs", "locks", "--json"]).await).unwrap();
    assert!(after_unlock.as_array().unwrap().is_empty());

    // Exercise actual transfer with locking verification enabled, then discard
    // the client's cache so a successful checkout must download from Shardline.
    let origin = tempfile::tempdir().unwrap();
    git(origin.path(), &["init", "--bare"]).await;
    git(
        worktree.path(),
        &[
            "remote",
            "set-url",
            "origin",
            origin.path().to_str().unwrap(),
        ],
    )
    .await;
    git(worktree.path(), &["lfs", "install", "--local"]).await;
    git(worktree.path(), &["lfs", "track", "*.bin"]).await;
    git(worktree.path(), &["add", ".gitattributes", "model.bin"]).await;
    git(
        worktree.path(),
        &[
            "-c",
            "user.name=Protocol Test",
            "-c",
            "user.email=protocol@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "LFS transfer fixture",
        ],
    )
    .await;
    git(worktree.path(), &["push", "origin", "main"]).await;
    std::fs::remove_dir_all(worktree.path().join(".git/lfs/objects")).unwrap();
    std::fs::remove_file(worktree.path().join("model.bin")).unwrap();
    git(worktree.path(), &["lfs", "fetch", "--all", "origin"]).await;
    git(worktree.path(), &["lfs", "checkout"]).await;
    assert_eq!(
        std::fs::read(worktree.path().join("model.bin")).unwrap(),
        b"native locking fixture"
    );
    server.stop().await;
}
