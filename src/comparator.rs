// Copyright 2014 Tyler Neely
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use crocksdb_ffi::{self, DBComparator};
use libc::{c_char, c_int, c_uchar, c_void, size_t};
use std::ffi::CString;
use std::slice;

pub struct ComparatorCallback {
    pub name: CString,
    pub compare_fn: fn(&[u8], &[u8]) -> i32,
}

pub unsafe extern "C" fn destructor_callback(raw_cb: *mut c_void) {
    // turn this back into a local variable so rust will reclaim it
    let _ = Box::from_raw(raw_cb as *mut ComparatorCallback);
}

pub unsafe extern "C" fn name_callback(raw_cb: *mut c_void) -> *const c_char {
    let cb: &mut ComparatorCallback = &mut *(raw_cb as *mut ComparatorCallback);
    let ptr = cb.name.as_ptr();
    ptr as *const c_char
}

pub unsafe extern "C" fn compare_callback(
    raw_cb: *mut c_void,
    a_raw: *const c_char,
    a_len: size_t,
    b_raw: *const c_char,
    b_len: size_t,
) -> c_int {
    let cb: &mut ComparatorCallback = &mut *(raw_cb as *mut ComparatorCallback);
    let a: &[u8] = slice::from_raw_parts(a_raw as *const u8, a_len as usize);
    let b: &[u8] = slice::from_raw_parts(b_raw as *const u8, b_len as usize);
    (cb.compare_fn)(a, b)
}

pub trait TimestampAwareComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> i32;
    fn compare_timestamp(&self, a: &[u8], b: &[u8]) -> i32;
    fn compare_without_timestamp(&self, a: &[u8], a_has_ts: bool, b: &[u8], b_has_ts: bool) -> i32;
}

struct TimestampAwareComparatorProxy<C: TimestampAwareComparator> {
    name: CString,
    comparator: C,
}

extern "C" fn name<C: TimestampAwareComparator>(comparator_proxy: *mut c_void) -> *const c_char {
    unsafe {
        (*(comparator_proxy as *mut TimestampAwareComparatorProxy<C>))
            .name
            .as_ptr()
    }
}

extern "C" fn destructor<C: TimestampAwareComparator>(comparator_proxy: *mut c_void) {
    unsafe {
        Box::from_raw(comparator_proxy as *mut TimestampAwareComparatorProxy<C>);
    }
}

extern "C" fn compare<C: TimestampAwareComparator>(
    comparator_proxy: *mut c_void,
    a_raw: *const c_char,
    a_len: size_t,
    b_raw: *const c_char,
    b_len: size_t,
) -> c_int {
    unsafe {
        let comparator = &(*(comparator_proxy as *mut TimestampAwareComparatorProxy<C>)).comparator;
        let a: &[u8] = slice::from_raw_parts(a_raw as *const u8, a_len as usize);
        let b: &[u8] = slice::from_raw_parts(b_raw as *const u8, b_len as usize);
        comparator.compare(a, b)
    }
}

extern "C" fn compare_ts<C: TimestampAwareComparator>(
    comparator_proxy: *mut c_void,
    a_raw: *const c_char,
    a_len: size_t,
    b_raw: *const c_char,
    b_len: size_t,
) -> c_int {
    unsafe {
        let comparator = &(*(comparator_proxy as *mut TimestampAwareComparatorProxy<C>)).comparator;
        let a: &[u8] = slice::from_raw_parts(a_raw as *const u8, a_len as usize);
        let b: &[u8] = slice::from_raw_parts(b_raw as *const u8, b_len as usize);
        comparator.compare_timestamp(a, b)
    }
}

extern "C" fn compare_without_ts<C: TimestampAwareComparator>(
    comparator_proxy: *mut c_void,
    a_raw: *const c_char,
    a_len: size_t,
    a_has_ts_raw: c_uchar,
    b_raw: *const c_char,
    b_len: size_t,
    b_has_ts_raw: c_uchar,
) -> c_int {
    unsafe {
        let comparator = &(*(comparator_proxy as *mut TimestampAwareComparatorProxy<C>)).comparator;
        let a: &[u8] = slice::from_raw_parts(a_raw as *const u8, a_len as usize);
        let a_has_ts = a_has_ts_raw != 0;
        let b: &[u8] = slice::from_raw_parts(b_raw as *const u8, b_len as usize);
        let b_has_ts = b_has_ts_raw != 0;
        comparator.compare_without_timestamp(a, a_has_ts, b, b_has_ts)
    }
}

pub unsafe fn new_timestamp_aware_comparator<S: Into<Vec<u8>>, C: TimestampAwareComparator>(
    comparator_name: S,
    ts_sz: usize,
    comparator: C,
) -> Result<ComparatorRAIIWrapper, String> {
    let c_name = match CString::new(comparator_name) {
        Ok(s) => s,
        Err(e) => return Err(format!("failed to convert to cstring: {:?}", e)),
    };

    let state = Box::into_raw(Box::new(TimestampAwareComparatorProxy {
        name: c_name,
        comparator,
    })) as *mut c_void;
    let db_comparator = crocksdb_ffi::crocksdb_comparator_create(
        state,
        ts_sz,
        destructor::<C>,
        compare::<C>,
        Some(compare_ts::<C>),
        Some(compare_without_ts::<C>),
        name::<C>,
    );
    Ok(ComparatorRAIIWrapper {
        inner: db_comparator,
    })
}

// RAII handle
pub struct ComparatorRAIIWrapper {
    pub inner: *mut DBComparator,
}

impl Drop for ComparatorRAIIWrapper {
    fn drop(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_comparator_destroy(self.inner);
        }
    }
}
