// Experimental CSV tokenizer for the native pipeline benchmark.
#[repr(C)]
pub struct Field {
    offset: usize,
    len: usize,
}

// The caller supplies a readable line and space for count fields.
// Fields borrow the line and are used only if this function succeeds.
#[no_mangle]
pub unsafe extern "C" fn rust_csv_fields(
    data: *const u8,
    len: usize,
    fields: *mut Field,
    count: usize,
) -> bool {
    let line = std::slice::from_raw_parts(data, len);
    let mut start = 0;
    let mut index = 0;
    let mut quotes = 0;
    for end in 0..=len {
        if end < len && line[end] != b',' {
            quotes += usize::from(line[end] == b'"');
            continue;
        }
        if index == count {
            return false;
        }
        let quoted = quotes == 2 && end - start >= 2
            && line[start] == b'"' && line[end - 1] == b'"';
        if quotes != 0 && !quoted {
            return false;
        }
        let trim = usize::from(quoted);
        fields.add(index).write(Field {
            offset: start + trim,
            len: end - start - 2 * trim,
        });
        index += 1;
        start = end + 1;
        quotes = 0;
    }
    index == count
}
