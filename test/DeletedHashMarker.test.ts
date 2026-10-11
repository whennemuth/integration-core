import { isDeletedHash, toDeletedHash } from '../src/delta-strategy/DeletedHashMarker';

describe('DeletedHashMarker', () => {
  it('prefixes a hash idempotently and recognizes marked hashes', () => {
    expect(toDeletedHash('abc')).toBe('DELETED:abc');
    expect(toDeletedHash('DELETED:abc')).toBe('DELETED:abc');
    expect(isDeletedHash('DELETED:abc')).toBe(true);
    expect(isDeletedHash('abc')).toBe(false);
    expect(isDeletedHash(undefined)).toBe(false);
  });
});
