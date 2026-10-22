// Native MinIO signer interoperability fixture, not a Shardline signer.
package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"flag"
	"fmt"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"io"
	"math/bits"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/minio/minio-go/v7/pkg/signer"
)

type signingHash struct{ hash.Hash }

func (signingHash) Close() {}

func main() {
	trailer := flag.Bool("trailer", false, "send a signed CRC32 trailer")
	algorithm := flag.String("checksum", "crc32", "signed trailer checksum: crc32, crc32c, crc64nvme, sha1, sha256")
	badChecksum := flag.Bool("bad-checksum", false, "sign a deliberately incorrect trailer checksum")
	corrupt := flag.Bool("corrupt", false, "corrupt an encoded payload byte")
	disconnect := flag.Bool("disconnect", false, "half-close the HTTP connection halfway through the signed body")
	sessionToken := flag.String("session-token", "", "session credential supplied to the native signer")
	expectDenied := flag.Bool("expect-denied", false, "require an authentication rejection")
	flag.Parse()
	if err := upload(flag.Arg(0), *trailer, *corrupt, *algorithm, *badChecksum, *disconnect, *sessionToken, *expectDenied); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func upload(endpoint string, trailer, corrupt bool, algorithm string, badChecksum, disconnect bool, sessionToken string, expectDenied bool) error {
	payload := bytes.Repeat([]byte("native MinIO signed streaming payload\n"), 5000)
	request, err := http.NewRequest(http.MethodPut, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Content-Encoding", "aws-chunked")
	if trailer {
		var digest hash.Hash
		switch algorithm {
		case "crc32":
			digest = crc32.NewIEEE()
		case "crc32c":
			digest = crc32.New(crc32.MakeTable(crc32.Castagnoli))
		case "crc64nvme":
			digest = crc64.New(crc64.MakeTable(bits.Reverse64(0xad93d23594c93659)))
		case "sha1":
			digest = sha1.New()
		case "sha256":
			digest = sha256.New()
		default:
			return fmt.Errorf("unsupported checksum %q", algorithm)
		}
		digest.Write(payload)
		checksum := digest.Sum(nil)
		if badChecksum {
			checksum[0] ^= 1
		}
		request.Trailer = http.Header{"X-Amz-Checksum-" + algorithm: {base64.StdEncoding.EncodeToString(checksum)}}
	}
	request = signer.StreamingSignV4(request, "SIGNINGKEY", "legacy-token-bridge", sessionToken, "us-east-1", int64(len(payload)), time.Now().UTC(), signingHash{sha256.New()})
	wire, err := io.ReadAll(request.Body)
	request.Body.Close()
	if err != nil {
		return err
	}
	if corrupt {
		position := bytes.Index(wire, []byte("\r\n"))
		if position < 0 {
			return fmt.Errorf("native producer emitted no chunk header")
		}
		wire[position+2] ^= 1
	}
	request.Body = io.NopCloser(bytes.NewReader(wire))
	request.ContentLength = int64(len(wire))
	if disconnect {
		return interrupt(request, wire)
	}
	client := &http.Client{Timeout: 20 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	expected := http.StatusOK
	if corrupt || expectDenied {
		expected = http.StatusForbidden
	} else if badChecksum {
		expected = http.StatusBadRequest
	}
	if response.StatusCode != expected {
		return fmt.Errorf("native upload status=%d expected=%d body=%s", response.StatusCode, expected, body)
	}
	if badChecksum && !bytes.Contains(body, []byte("<Code>BadDigest</Code>")) {
		return fmt.Errorf("incorrect checksum error: %s", body)
	}
	fmt.Println(response.Header.Get("ETag"))
	return nil
}

// Half-close only the write direction so the fixture can observe server handling
// of the incomplete body rather than guessing from an elapsed delay.
func interrupt(request *http.Request, wire []byte) error {
	if request.URL.Scheme != "http" {
		return fmt.Errorf("disconnect fixture requires local plain HTTP")
	}
	connection, err := net.DialTimeout("tcp", request.URL.Host, 5*time.Second)
	if err != nil {
		return err
	}
	defer connection.Close()
	connection.SetDeadline(time.Now().Add(20 * time.Second))
	var encoded bytes.Buffer
	fmt.Fprintf(&encoded, "PUT %s HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\nConnection: close\r\n", request.URL.RequestURI(), request.URL.Host, len(wire))
	headers := request.Header.Clone()
	headers.Del("Content-Length")
	headers.Del("Connection")
	if err := headers.Write(&encoded); err != nil {
		return err
	}
	encoded.WriteString("\r\n")
	encoded.Write(wire[:len(wire)/2])
	if _, err := io.Copy(connection, &encoded); err != nil {
		return err
	}
	if err := connection.(*net.TCPConn).CloseWrite(); err != nil {
		return err
	}
	response, err := http.ReadResponse(bufio.NewReader(connection), request)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 400 {
		return fmt.Errorf("interrupted body accepted: %s", response.Status)
	}
	_, err = io.Copy(io.Discard, response.Body)
	return err
}
