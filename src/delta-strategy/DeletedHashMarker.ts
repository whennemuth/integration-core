/**
 * Marks a stored baseline hash as belonging to a person who has been soft-deleted (deactivated)
 * in the target. Applies to every delta storage type that keeps a baseline of person hashes.
 *
 * Why the marker lives in the hash itself:
 * Baselines (previous-input.ndjson for S3/file storage, PersonCurrentState for DynamoDB storage)
 * retain every person ever seen. Without a record of the deactivation, a person absent from the
 * source stays in the baseline and is identified for deletion again on every subsequent full sync.
 * Prefixing the hash serves both purposes that deactivation state needs to serve:
 *   1. Deferred delete handling skips marked baseline entries, so a person is deactivated once.
 *   2. A marked hash can never equal a freshly computed source hash, so a person who reappears in
 *      the source is classified UPDATED (not UNCHANGED), and that UPDATE reactivates them in the
 *      target. Writing the new hash on a successful push clears the marker.
 *
 * Defined here so that every writer of a baseline - e.g. a bulk sync's deferred delete handling,
 * or a sync of individual persons - marks deletions identically.
 */
export const DELETED_HASH_PREFIX = 'DELETED:';

export const isDeletedHash = (hash?: string): boolean => {
  return typeof hash === 'string' && hash.startsWith(DELETED_HASH_PREFIX);
}

/**
 * Idempotent: an already marked hash is returned unchanged.
 */
export const toDeletedHash = (hash: string): string => {
  return isDeletedHash(hash) ? hash : `${DELETED_HASH_PREFIX}${hash}`;
}
