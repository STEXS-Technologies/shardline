/// Git smart-HTTP command request limits. Upload-pack negotiation is tiny;
/// receive-pack includes a compressed pack and is bounded independently from
/// the Hub's ordinary object-upload routes.
pub(crate) const MAX_UPLOAD_PACK_REQUEST_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_RECEIVE_PACK_REQUEST_BYTES: usize = 64 * 1024 * 1024;
