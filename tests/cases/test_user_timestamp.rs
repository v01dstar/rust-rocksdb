use std::mem::size_of;

use rocksdb::{
    ColumnFamilyOptions, DBOptions, EnvOptions, IngestExternalFileOptions, ReadOptions, SeekKey,
    SstFileWriter, TimestampAwareComparator, Writable, WriteBatch, DB,
};

use super::tempdir_with_prefix;
struct ComparatorWithU64 {
    timestamp_size: usize,
}

impl ComparatorWithU64 {
    fn new() -> Self {
        ComparatorWithU64 {
            timestamp_size: size_of::<u64>() / size_of::<u8>(),
        }
    }
}

#[inline]
fn extract_timestamp_from_user_key<'a>(key: &'a [u8], timestamp_size: &'a usize) -> &'a [u8] {
    &key[key.len() - timestamp_size..]
}
#[inline]
fn strip_timestamp_from_user_key<'a>(key: &'a [u8], timestamp_size: &'a usize) -> &'a [u8] {
    &key[..key.len() - timestamp_size]
}
impl TimestampAwareComparator for ComparatorWithU64 {
    fn compare(&self, a: &[u8], b: &[u8]) -> i32 {
        let ret = self.compare_without_timestamp(a, true, b, true);
        if ret != 0 {
            return ret;
        }
        return -self.compare_timestamp(
            extract_timestamp_from_user_key(a, &self.timestamp_size),
            extract_timestamp_from_user_key(b, &self.timestamp_size),
        );
    }
    fn compare_timestamp(&self, a: &[u8], b: &[u8]) -> i32 {
        a.cmp(b) as i32
    }
    fn compare_without_timestamp(&self, a: &[u8], a_has_ts: bool, b: &[u8], b_has_ts: bool) -> i32 {
        assert!(!a_has_ts || a.len() > self.timestamp_size);
        assert!(!b_has_ts || b.len() > self.timestamp_size);
        let raw_a = if a_has_ts {
            strip_timestamp_from_user_key(a, &self.timestamp_size)
        } else {
            a
        };
        let raw_b = if b_has_ts {
            strip_timestamp_from_user_key(b, &self.timestamp_size)
        } else {
            b
        };
        raw_a.cmp(raw_b) as i32
    }
}

#[test]
fn test_user_timestamp_read_write() {
    let temp = tempdir_with_prefix("_rust_rocksdb_test_user_timestamp_read_write");
    let path = temp.path().to_str().unwrap();
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    let mut cf_opts = ColumnFamilyOptions::new();
    let _ = cf_opts.add_timestamp_aware_comparator(
        "rust-rocksdb.bytewise-comparator-with-u64-ts",
        8,
        ComparatorWithU64::new(),
    );
    let db = DB::open_cf(opts, path, vec![("default", cf_opts)]).unwrap();

    db.put_with_ts(b"k1", &1u64.to_be_bytes(), b"v1").unwrap();
    db.put_with_ts(b"k1", &2u64.to_be_bytes(), b"v12").unwrap();
    db.put_with_ts(b"k2", &1u64.to_be_bytes(), b"v2").unwrap();
    db.delete_with_ts(b"k2", &2u64.to_be_bytes()).unwrap();
    db.put_with_ts(b"k3", &1u64.to_be_bytes(), b"v3").unwrap();
    db.put_with_ts(b"k3", &2u64.to_be_bytes(), b"v32").unwrap();

    let mut read_opts = ReadOptions::new();
    read_opts.set_timestamp(2u64.to_be_bytes().to_vec());
    assert_eq!(db.get_opt(b"k1", &read_opts).unwrap().unwrap(), b"v12");
    assert!(db.get_opt(b"k2", &read_opts).unwrap().is_none());
    assert_eq!(db.get_opt(b"k3", &read_opts).unwrap().unwrap(), b"v32");

    read_opts.set_timestamp(1u64.to_be_bytes().to_vec());
    assert_eq!(db.get_opt(b"k1", &read_opts).unwrap().unwrap(), b"v1");
    assert_eq!(db.get_opt(b"k2", &read_opts).unwrap().unwrap(), b"v2");
    assert_eq!(db.get_opt(b"k3", &read_opts).unwrap().unwrap(), b"v3");

    db.put_with_ts(b"k1", &1u64.to_be_bytes(), b"v13").unwrap();
    db.put_with_ts(b"k2", &1u64.to_be_bytes(), b"v23").unwrap();
    db.put_with_ts(b"k3", &1u64.to_be_bytes(), b"v33").unwrap();
    read_opts.set_timestamp(1u64.to_be_bytes().to_vec());
    assert_eq!(db.get_opt(b"k1", &read_opts).unwrap().unwrap(), b"v13");
    assert_eq!(db.get_opt(b"k2", &read_opts).unwrap().unwrap(), b"v23");
    assert_eq!(db.get_opt(b"k3", &read_opts).unwrap().unwrap(), b"v33");
}

#[test]
fn test_user_timestamp_write_batch() {
    let temp = tempdir_with_prefix("_rust_rocksdb_test_user_timestamp_write_batch");
    let path = temp.path().to_str().unwrap();
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    let mut cf_opts = ColumnFamilyOptions::new();
    let _ = cf_opts.add_timestamp_aware_comparator(
        "rust-rocksdb.bytewise-comparator-with-u64-ts",
        8,
        ComparatorWithU64::new(),
    );
    let db = DB::open_cf(opts, path, vec![("default", cf_opts)]).unwrap();
    let cf_handle = db.cf_handle("default").unwrap();
    let wb = WriteBatch::new();
    wb.put_cf_with_ts(cf_handle, b"k1", &1u64.to_be_bytes(), b"v1")
        .unwrap();
    wb.put_cf_with_ts(cf_handle, b"k2", &1u64.to_be_bytes(), b"v2")
        .unwrap();
    wb.put_cf_with_ts(cf_handle, b"k3", &1u64.to_be_bytes(), b"v3")
        .unwrap();
    wb.put_cf_with_ts(cf_handle, b"k1", &2u64.to_be_bytes(), b"v12")
        .unwrap();
    wb.delete_cf_with_ts(cf_handle, b"k2", &2u64.to_be_bytes())
        .unwrap();
    wb.put_cf_with_ts(cf_handle, b"k4", &2u64.to_be_bytes(), b"v4")
        .unwrap();
    wb.put_cf_with_ts(cf_handle, b"k5", &2u64.to_be_bytes(), b"v5")
        .unwrap();
    db.write(&wb).unwrap();

    let mut read_opts = ReadOptions::new();
    read_opts.set_timestamp(2u64.to_be_bytes().to_vec());
    assert_eq!(db.get_opt(b"k1", &read_opts).unwrap().unwrap(), b"v12");
    let a = db.get_opt(b"k1", &read_opts).unwrap();
    assert!(db.get_opt(b"k2", &read_opts).unwrap().is_none());
    assert_eq!(db.get_opt(b"k3", &read_opts).unwrap().unwrap(), b"v3");

    read_opts.set_timestamp(1u64.to_be_bytes().to_vec());
    assert_eq!(db.get_opt(b"k1", &read_opts).unwrap().unwrap(), b"v1");
    assert_eq!(db.get_opt(b"k2", &read_opts).unwrap().unwrap(), b"v2");
    assert_eq!(db.get_opt(b"k3", &read_opts).unwrap().unwrap(), b"v3");
}

fn create_db_with_timestamp_aware_column_family(db_path: &str, cf_name: &str) -> DB {
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    let mut cf_opts = ColumnFamilyOptions::new();
    let _ = cf_opts.add_timestamp_aware_comparator(
        "rust-rocksdb.bytewise-comparator-with-u64-ts",
        8,
        ComparatorWithU64::new(),
    );
    let mut db = DB::open_cf(opts, db_path, vec![("default", cf_opts)]).unwrap();
    let mut cf_opts = ColumnFamilyOptions::new();
    let _ = cf_opts.add_timestamp_aware_comparator(
        "rust-rocksdb.bytewise-comparator-with-u64-ts",
        8,
        ComparatorWithU64::new(),
    );
    let _ = db.create_cf((cf_name, cf_opts)).unwrap();
    db
}

#[test]
fn test_user_timestamp_non_default_column_family() {
    let temp = tempdir_with_prefix("_rust_rocksdb_test_user_timestamp_non_default_column_family");
    let path = temp.path().to_str().unwrap();
    let db = create_db_with_timestamp_aware_column_family(path, "write");
    let cf_handle = db.cf_handle("write").unwrap();

    db.put_cf_with_ts(cf_handle, b"k1", &1u64.to_be_bytes(), b"v1")
        .unwrap();
    db.put_cf_with_ts(cf_handle, b"k1", &2u64.to_be_bytes(), b"v12")
        .unwrap();
    db.put_cf_with_ts(cf_handle, b"k2", &1u64.to_be_bytes(), b"v2")
        .unwrap();
    db.delete_cf_with_ts(cf_handle, b"k2", &2u64.to_be_bytes())
        .unwrap();
    db.put_cf_with_ts(cf_handle, b"k3", &1u64.to_be_bytes(), b"v3")
        .unwrap();
    db.put_cf_with_ts(cf_handle, b"k3", &2u64.to_be_bytes(), b"v32")
        .unwrap();

    let mut read_opts = ReadOptions::new();
    read_opts.set_timestamp(2u64.to_be_bytes().to_vec());
    assert_eq!(
        db.get_cf_opt(cf_handle, b"k1", &read_opts)
            .unwrap()
            .unwrap(),
        b"v12"
    );
    assert!(db
        .get_cf_opt(cf_handle, b"k2", &read_opts)
        .unwrap()
        .is_none());
    assert_eq!(
        db.get_cf_opt(cf_handle, b"k3", &read_opts)
            .unwrap()
            .unwrap(),
        b"v32"
    );

    read_opts.set_timestamp(1u64.to_be_bytes().to_vec());
    assert_eq!(
        db.get_cf_opt(cf_handle, b"k1", &read_opts)
            .unwrap()
            .unwrap(),
        b"v1"
    );
    assert_eq!(
        db.get_cf_opt(cf_handle, b"k2", &read_opts)
            .unwrap()
            .unwrap(),
        b"v2"
    );
    assert_eq!(
        db.get_cf_opt(cf_handle, b"k3", &read_opts)
            .unwrap()
            .unwrap(),
        b"v3"
    );
}

#[test]
fn test_user_timestamp_iterator() {
    let temp = tempdir_with_prefix("_rust_rocksdb_test_user_timestamp_iterator");
    let path = temp.path().to_str().unwrap();
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    let mut cf_opts = ColumnFamilyOptions::new();
    let _ = cf_opts.add_timestamp_aware_comparator(
        "rust-rocksdb.bytewise-comparator-with-u64-ts",
        8,
        ComparatorWithU64::new(),
    );
    let db = DB::open_cf(opts, path, vec![("default", cf_opts)]).unwrap();

    db.put_with_ts(b"k1", &1u64.to_be_bytes(), b"v11").unwrap();
    db.put_with_ts(b"k1", &2u64.to_be_bytes(), b"v12").unwrap();
    db.put_with_ts(b"k2", &1u64.to_be_bytes(), b"v21").unwrap();
    db.delete_with_ts(b"k2", &2u64.to_be_bytes()).unwrap();
    db.put_with_ts(b"k3", &1u64.to_be_bytes(), b"v31").unwrap();
    db.put_with_ts(b"k3", &2u64.to_be_bytes(), b"v32").unwrap();

    let mut read_opts = ReadOptions::new();
    read_opts.set_timestamp(1u64.to_be_bytes().to_vec());
    let mut iter = db.iter_opt(read_opts);
    iter.seek(SeekKey::Start).unwrap();
    assert_eq!(iter.key(), b"k1");
    assert_eq!(iter.ts().unwrap(), 1u64.to_be_bytes());
    assert_eq!(iter.value(), b"v11");
    iter.next().unwrap();
    assert_eq!(iter.key(), b"k2");
    assert_eq!(iter.ts().unwrap(), 1u64.to_be_bytes());
    assert_eq!(iter.value(), b"v21");
    iter.next().unwrap();
    assert_eq!(iter.key(), b"k3");
    assert_eq!(iter.ts().unwrap(), 1u64.to_be_bytes());
    assert_eq!(iter.value(), b"v31");

    let mut read_opts = ReadOptions::new();
    read_opts.set_timestamp(2u64.to_be_bytes().to_vec());
    let mut iter = db.iter_opt(read_opts);
    iter.seek(SeekKey::Start).unwrap();
    assert_eq!(iter.key(), b"k1");
    assert_eq!(iter.ts().unwrap(), 2u64.to_be_bytes());
    assert_eq!(iter.value(), b"v12");
    iter.next().unwrap();
    assert_eq!(iter.key(), b"k3");
    assert_eq!(iter.ts().unwrap(), 2u64.to_be_bytes());
    assert_eq!(iter.value(), b"v32");
}

#[test]
fn test_user_timestamp_iterator_with_start_ts() {
    let temp = tempdir_with_prefix("_rust_rocksdb_test_user_timestamp_iterator_with_start_ts");
    let path = temp.path().to_str().unwrap();
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    let mut cf_opts = ColumnFamilyOptions::new();
    let _ = cf_opts.add_timestamp_aware_comparator(
        "rust-rocksdb.bytewise-comparator-with-u64-ts",
        8,
        ComparatorWithU64::new(),
    );
    let db = DB::open_cf(opts, path, vec![("default", cf_opts)]).unwrap();

    db.put_with_ts(b"k1", &1u64.to_be_bytes(), b"v11").unwrap();
    db.put_with_ts(b"k1", &2u64.to_be_bytes(), b"v12").unwrap();
    db.put_with_ts(b"k2", &1u64.to_be_bytes(), b"v21").unwrap();
    db.delete_with_ts(b"k2", &2u64.to_be_bytes()).unwrap();
    db.put_with_ts(b"k3", &1u64.to_be_bytes(), b"v31").unwrap();
    db.put_with_ts(b"k3", &2u64.to_be_bytes(), b"v32").unwrap();

    let mut read_opts = ReadOptions::new();
    read_opts.set_timestamp(2u64.to_be_bytes().to_vec());
    read_opts.set_iter_start_ts(1u64.to_be_bytes().to_vec());
    let mut iter = db.iter_opt(read_opts);
    iter.seek(SeekKey::Start).unwrap();
    assert_eq!(iter.key(), b"k1");
    assert_eq!(iter.ts().unwrap(), 2u64.to_be_bytes());
    assert_eq!(iter.value(), b"v12");
    iter.next().unwrap();
    assert_eq!(iter.key(), b"k1");
    assert_eq!(iter.ts().unwrap(), 1u64.to_be_bytes());
    assert_eq!(iter.value(), b"v11");
    iter.next().unwrap();
    assert_eq!(iter.key(), b"k2");
    assert_eq!(iter.ts().unwrap(), 2u64.to_be_bytes());
    assert_eq!(iter.value(), b"");
    iter.next().unwrap();
    assert_eq!(iter.key(), b"k2");
    assert_eq!(iter.ts().unwrap(), 1u64.to_be_bytes());
    assert_eq!(iter.value(), b"v21");
    iter.next().unwrap();
    assert_eq!(iter.key(), b"k3");
    assert_eq!(iter.ts().unwrap(), 2u64.to_be_bytes());
    assert_eq!(iter.value(), b"v32");
    iter.next().unwrap();
    assert_eq!(iter.key(), b"k3");
    assert_eq!(iter.ts().unwrap(), 1u64.to_be_bytes());
    assert_eq!(iter.value(), b"v31");
}

#[test]
fn test_user_timestamp_iterator_seek_prev() {
    let temp = tempdir_with_prefix("_rust_rocksdb_test_user_timestamp_iterator_seek_prev");
    let path = temp.path().to_str().unwrap();
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    let mut cf_opts = ColumnFamilyOptions::new();
    let _ = cf_opts.add_timestamp_aware_comparator(
        "rust-rocksdb.bytewise-comparator-with-u64-ts",
        8,
        ComparatorWithU64::new(),
    );
    let db = DB::open_cf(opts, path, vec![("default", cf_opts)]).unwrap();

    db.put_with_ts(b"k1", &1u64.to_be_bytes(), b"v11").unwrap();
    db.put_with_ts(b"k1", &2u64.to_be_bytes(), b"v12").unwrap();
    db.put_with_ts(b"k2", &1u64.to_be_bytes(), b"v21").unwrap();
    db.delete_with_ts(b"k2", &2u64.to_be_bytes()).unwrap();
    db.put_with_ts(b"k3", &1u64.to_be_bytes(), b"v31").unwrap();
    db.put_with_ts(b"k3", &2u64.to_be_bytes(), b"v32").unwrap();

    let mut read_opts = ReadOptions::new();
    read_opts.set_timestamp(2u64.to_be_bytes().to_vec());
    read_opts.set_iter_start_ts(1u64.to_be_bytes().to_vec());
    let mut iter = db.iter_opt(read_opts);
    iter.seek_for_prev(SeekKey::Key(b"k3")).unwrap();
    assert_eq!(iter.key(), b"k3");
    assert_eq!(iter.ts().unwrap(), 1u64.to_be_bytes());
    assert_eq!(iter.value(), b"v31");
    iter.prev().unwrap();
    assert_eq!(iter.key(), b"k3");
    assert_eq!(iter.ts().unwrap(), 2u64.to_be_bytes());
    assert_eq!(iter.value(), b"v32");
    iter.prev().unwrap();
    assert_eq!(iter.key(), b"k2");
    assert_eq!(iter.ts().unwrap(), 1u64.to_be_bytes());
    assert_eq!(iter.value(), b"v21");
    iter.prev().unwrap();
    assert_eq!(iter.key(), b"k2");
    assert_eq!(iter.ts().unwrap(), 2u64.to_be_bytes());
    assert_eq!(iter.value(), b"");
    iter.prev().unwrap();
    assert_eq!(iter.key(), b"k1");
    assert_eq!(iter.ts().unwrap(), 1u64.to_be_bytes());
    assert_eq!(iter.value(), b"v11");
    iter.prev().unwrap();
    assert_eq!(iter.key(), b"k1");
    assert_eq!(iter.ts().unwrap(), 2u64.to_be_bytes());
    assert_eq!(iter.value(), b"v12");
}

#[test]
fn test_user_timestamp_sst() {
    let path_dir = tempdir_with_prefix("_rust_rocksdb_test_user_timestamp_sst");
    let root_path = path_dir.path();
    let db_path_buf = root_path.join("db");
    let db_path = db_path_buf.to_str().unwrap();
    let db = create_db_with_timestamp_aware_column_family(db_path, "write");

    let cf_handle = db.cf_handle("default").unwrap();
    db.put_cf_with_ts(cf_handle, b"k1", &1u64.to_be_bytes(), b"v1")
        .unwrap();
    db.put_cf_with_ts(cf_handle, b"k1", &2u64.to_be_bytes(), b"v12")
        .unwrap();
    db.put_cf_with_ts(cf_handle, b"k2", &1u64.to_be_bytes(), b"v2")
        .unwrap();
    db.delete_cf_with_ts(cf_handle, b"k2", &2u64.to_be_bytes())
        .unwrap();
    db.put_cf_with_ts(cf_handle, b"k3", &1u64.to_be_bytes(), b"v3")
        .unwrap();
    db.put_cf_with_ts(cf_handle, b"k3", &2u64.to_be_bytes(), b"v32")
        .unwrap();

    let sst_path_buf1 = root_path.join("sst1");
    let sst_path_buf2 = root_path.join("sst2");
    let sst_path1 = sst_path_buf1.to_str().unwrap();
    let sst_path2 = sst_path_buf2.to_str().unwrap();
    let mut opts = ColumnFamilyOptions::new();
    let _ = opts.add_timestamp_aware_comparator(
        "rust-rocksdb.bytewise-comparator-with-u64-ts",
        8,
        ComparatorWithU64::new(),
    );
    let mut sst1 = SstFileWriter::new(EnvOptions::new(), opts.clone());
    let mut sst2 = SstFileWriter::new(EnvOptions::new(), opts.clone());
    sst1.open(sst_path1).unwrap();
    sst2.open(sst_path2).unwrap();

    for sst in [&mut sst1, &mut sst2] {
        sst.put_with_ts(b"k1", &2u64.to_be_bytes(), b"v12").unwrap();
        sst.put_with_ts(b"k1", &1u64.to_be_bytes(), b"v11").unwrap();
        sst.delete_with_ts(b"k2", &2u64.to_be_bytes()).unwrap();
        sst.put_with_ts(b"k2", &1u64.to_be_bytes(), b"v21").unwrap();
        sst.put_with_ts(b"k3", &2u64.to_be_bytes(), b"v32").unwrap();
        sst.put_with_ts(b"k3", &1u64.to_be_bytes(), b"v31").unwrap();
    }
    sst1.finish().unwrap();
    sst2.finish().unwrap();
    let mut ingest_opt = IngestExternalFileOptions::new();
    ingest_opt.move_files(true);
    db.ingest_external_file_cf(db.cf_handle("default").unwrap(), &ingest_opt, &[sst_path1])
        .unwrap();
    db.ingest_external_file_cf(db.cf_handle("write").unwrap(), &ingest_opt, &[sst_path2])
        .unwrap();
    for cf_name in ["default", "write"] {
        let cf = db.cf_handle(cf_name).unwrap();

        let mut read_opts = ReadOptions::new();
        read_opts.set_timestamp(2u64.to_be_bytes().to_vec());
        assert_eq!(
            db.get_cf_opt(cf, b"k1", &read_opts).unwrap().unwrap(),
            b"v12"
        );
        assert!(db.get_cf_opt(cf, b"k2", &read_opts).unwrap().is_none());
        assert_eq!(
            db.get_cf_opt(cf, b"k3", &read_opts).unwrap().unwrap(),
            b"v32"
        );

        read_opts.set_timestamp(1u64.to_be_bytes().to_vec());
        assert_eq!(
            db.get_cf_opt(cf, b"k1", &read_opts).unwrap().unwrap(),
            b"v11"
        );
        assert_eq!(
            db.get_cf_opt(cf, b"k2", &read_opts).unwrap().unwrap(),
            b"v21"
        );
        assert_eq!(
            db.get_cf_opt(cf, b"k3", &read_opts).unwrap().unwrap(),
            b"v31"
        );
    }
}

#[test]
fn test_user_timestamp_get_val_and_ts() {
    let temp = tempdir_with_prefix("_rust_rocksdb_test_user_timestamp_get_val_and_ts");
    let path = temp.path().to_str().unwrap();
    let db = create_db_with_timestamp_aware_column_family(path, "write");
    let cf_handle = db.cf_handle("write").unwrap();

    db.put_cf_with_ts(cf_handle, b"k1", &1u64.to_be_bytes(), b"v1")
        .unwrap();
    db.put_cf_with_ts(cf_handle, b"k1", &3u64.to_be_bytes(), b"v12")
        .unwrap();
    db.put_cf_with_ts(cf_handle, b"k2", &1u64.to_be_bytes(), b"v2")
        .unwrap();
    db.delete_cf_with_ts(cf_handle, b"k2", &2u64.to_be_bytes())
        .unwrap();
    db.put_cf_with_ts(cf_handle, b"k3", &1u64.to_be_bytes(), b"v3")
        .unwrap();
    db.put_cf_with_ts(cf_handle, b"k3", &5u64.to_be_bytes(), b"v32")
        .unwrap();

    let mut read_opts = ReadOptions::new();
    read_opts.set_timestamp(10u64.to_be_bytes().to_vec());
    let (mut val, mut ts) = db
        .get_cf_opt_ts(cf_handle, b"k1", &read_opts)
        .unwrap()
        .unwrap();
    assert_eq!(val, b"v12");
    assert_eq!(ts, 3u64.to_be_bytes().to_vec());
    assert!(db
        .get_cf_opt_ts(cf_handle, b"k2", &read_opts)
        .unwrap()
        .is_none());
    (val, ts) = db
        .get_cf_opt_ts(cf_handle, b"k3", &read_opts)
        .unwrap()
        .unwrap();
    assert_eq!(val, b"v32");
    assert_eq!(ts, 5u64.to_be_bytes().to_vec());

    read_opts.set_timestamp(1u64.to_be_bytes().to_vec());
    (val, ts) = db
        .get_cf_opt_ts(cf_handle, b"k1", &read_opts)
        .unwrap()
        .unwrap();
    assert_eq!(val, b"v1");
    assert_eq!(ts, 1u64.to_be_bytes().to_vec());
    (val, ts) = db
        .get_cf_opt_ts(cf_handle, b"k2", &read_opts)
        .unwrap()
        .unwrap();
    assert_eq!(val, b"v2");
    assert_eq!(ts, 1u64.to_be_bytes().to_vec());
    (val, ts) = db
        .get_cf_opt_ts(cf_handle, b"k3", &read_opts)
        .unwrap()
        .unwrap();
    assert_eq!(val, b"v3");
    assert_eq!(ts, 1u64.to_be_bytes().to_vec());
}
