use super::*;

#[derive(Clone, Copy)]
enum Width {
    Mid(u8),
    Large(u16),
    Extra(u32),
}

pub struct RowEncoding {
    flag: u8,
    offsets: Vec<u8>,
    buf: Vec<u8>,
}

impl RowEncoding {
    pub fn new() -> Self {
        RowEncoding {
            flag: 0b_1000_0000,
            offsets: vec![],
            buf: vec![],
        }
    }

    fn set_width(&mut self, datum_num: &Width) {
        match datum_num {
            Width::Mid(_) => {
                self.flag |= 0b0100;
            }
            Width::Large(_) => {
                self.flag |= 0b1000;
            }
            Width::Extra(_) => {
                self.flag |= 0b1100;
            }
        }
    }

    pub fn set_big(&mut self, maybe_offset: Vec<usize>, max_offset: usize) {
        self.offsets = vec![];
        match max_offset {
            n if n <= u8::MAX as usize => {
                self.flag |= 0b01;
                maybe_offset
                    .into_iter()
                    .for_each(|m| self.offsets.put_u8(m as u8));
            }
            n if n <= u16::MAX as usize => {
                self.flag |= 0b10;
                maybe_offset
                    .into_iter()
                    .for_each(|m| self.offsets.put_u16_le(m as u16));
            }
            n if n <= u32::MAX as usize => {
                self.flag |= 0b11;
                maybe_offset
                    .into_iter()
                    .for_each(|m| self.offsets.put_u32_le(m as u32));
            }
            _ => unreachable!("encoding length exceeds u32"),
        }
    }

    pub fn encode(
        &mut self,
        datum_refs: impl Iterator<Item = impl ToDatumRef>,
    ) {
        assert!(
            self.buf.is_empty(),
            "should not encode one RowEncoding object multiple times."
        );
        let mut maybe_offset = vec![];
        for datum in datum_refs {
            maybe_offset.push(self.buf.len());
            if let Some(v) = datum.to_datum_ref() {
                serialize_scalar(v, &mut self.buf);
            }
        }
        let max_offset = *maybe_offset
            .last()
            .expect("should encode at least one column");
        self.set_big(maybe_offset, max_offset);
    }
}

pub struct Serializer {
    encoded_column_ids: Vec<u8>,
    datum_num: Width,
    encoded_datum_num: Vec<u8>
}

impl Serializer {
    pub fn new(column_ids: &[ColumnId]) -> Self {
        let mut encoded_column_ids = Vec::with_capacity(column_ids.len() * 4);
        for id in column_ids {
            encoded_column_ids.put_i32_le(id.get_id());
        }
        let datum_num = match column_ids.len() {
            n if n <= u8::MAX as usize => {
                Width::Mid(n as u8)
            }
            n if n <= u16::MAX as usize => {
                Width::Large(n as u16)
            }
            n if n <= u32::MAX as usize => {
                Width::Extra(n as u32)
            }
            _ => unreachable!("the number of columns exceeds u32"),
        };
        let mut encoded_datum_num = vec![];
        serialize_width(datum_num, &mut encoded_datum_num);
        Self { encoded_column_ids, datum_num, encoded_datum_num }
    }

    pub fn serialize_row_column_aware(&self, row: impl Row) -> Vec<u8> {
        let mut encoding = RowEncoding::new();
        encoding.encode(row.iter());
        self.serialize(encoding)
    }

    fn serialize(&self, mut encoding: RowEncoding) -> Vec<u8> {
        let mut row_bytes = vec![];
        encoding.set_width(&self.datum_num);
        row_bytes.put_u8(encoding.flag);
        row_bytes.extend(self.encoded_datum_num.iter());
        row_bytes.extend(self.encoded_column_ids.iter());
        row_bytes.extend(encoding.offsets.iter());
        row_bytes.extend(encoding.buf.iter());

        row_bytes
    }
}

pub fn decode(
    need_columns: &[ColumnId],
    schema: &[DataType],
    mut encoded_bytes: &[u8],
) -> Vec<Datum> {
    let flag = encoded_bytes.get_u8();
    let nums_bytes = match flag & 0b1100 {
        0b0100 => 1,
        0b1000 => 2,
        0b1100 => 4,
        _ => unreachable!("flag's WW bits corrupted"),
    };
    let offset_bytes = match flag & 0b11 {
        0b01 => 1,
        0b10 => 2,
        0b11 => 4,
        _ => unreachable!("flag's BB bits corrupted"),
    };
    let non_null_datum_nums = deserialize_width(nums_bytes, &mut encoded_bytes);
    let id_to_index = (0..non_null_datum_nums)
        .into_iter()
        .fold(HashMap::new(), |mut map, i| {
            map.insert(ColumnId::new(encoded_bytes.get_i32_le()), i);
            map
        });
    // let column_id_start_idx = 1 + nums_bytes;
    let offsets_start_idx = 0;
    // let mut one_offset = &encoded_bytes[offsets_start_idx..offsets_start_idx+4];
    // let mut another_offset = &encoded_bytes[offsets_start_idx+4..offsets_start_idx+8];
    // let one_offset_val = one_offset.get_i32_le();
    let data_start_idx = offsets_start_idx + non_null_datum_nums * offset_bytes;
    let mut datums = vec![];
    datums.reserve(schema.len());
    for (id, datatype) in need_columns.iter().zip_eq(schema.iter()) {
        if let Some(&index) = id_to_index.get(id) {
            // let base_offset_idx = offsets_start_idx + 4 * index;
            let mut this_offset_slice = &encoded_bytes[index..(index + offset_bytes)];
            let this_offset = deserialize_width(offset_bytes, &mut this_offset_slice);
            let data = if index + 1 < data_start_idx {
                let mut next_offset_slice =
                    &encoded_bytes[(index + offset_bytes)..(index + 2 * offset_bytes)];
                let next_offset = deserialize_width(offset_bytes, &mut next_offset_slice);
                let mut data_slice =
                    &encoded_bytes[(data_start_idx + this_offset)..(data_start_idx + next_offset)];
                deserialize_value(datatype, &mut data_slice)
            } else {
                let mut data_slice = &encoded_bytes[(data_start_idx + this_offset)..];
                deserialize_value(datatype, &mut data_slice)
            };
            if let Ok(d) = data {
                datums.push(Some(d));
            } else {
                unreachable!("decode error");
            }
        } else {
            datums.push(None);
        }
    }

    datums
}

fn serialize_width(width: Width, buf: &mut impl BufMut) {
    match width {
        Width::Mid(w) => buf.put_u8(w),
        Width::Large(w) => buf.put_u16_le(w),
        Width::Extra(w) => buf.put_u32_le(w),
    }
}

fn deserialize_width(len: usize, data: &mut impl Buf) -> usize {
    match len {
        1 => data.get_u8() as usize,
        2 => data.get_u16_le() as usize,
        4 => data.get_u32_le() as usize,
        _ => unreachable!("Width's len should be either 1, 2, or 4"),
    }
}
