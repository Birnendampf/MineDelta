use pyo3::exceptions::{PyEOFError, PyOverflowError, PyValueError};
use pyo3::prelude::*;
use std::collections::HashMap;

#[derive(PartialEq)]
enum RawCompound<'a> {
    Mem(&'a [u8]),
    Map(HashMap<&'a [u8], RawCompound<'a>>),
    List(Vec<RawCompound<'a>>),
}
type ParseFuncType = for<'a> fn(&mut &'a [u8]) -> PyResult<RawCompound<'a>>;

const TAG_LUT: [Option<ParseFuncType>; 13] = [
    None,                       //  TAG_End
    Some(get_raw_numeric::<1>), //  TAG_Byte
    Some(get_raw_numeric::<2>), //  TAG_Short
    Some(get_raw_numeric::<4>), //  TAG_Int
    Some(get_raw_numeric::<8>), //  TAG_Long
    Some(get_raw_numeric::<4>), //  TAG_Float
    Some(get_raw_numeric::<8>), //  TAG_Double
    Some(get_raw_array::<1>),   //  TAG_Byte_Array
    Some(get_raw_string),       //  TAG_String
    Some(get_raw_list),         //  TAG_List
    Some(get_raw_compound),     //  TAG_Compound
    Some(get_raw_array::<4>),   //  TAG_Int_Array
    Some(get_raw_array::<8>),   //  TAG_Long_Array
];
const TAG_SIZE_LUT: [u8; 7] = [0, 1, 2, 4, 8, 4, 8];

fn get_raw_numeric<'a, const N: usize>(data: &mut &'a [u8]) -> PyResult<RawCompound<'a>> {
    let num = split_off(data, N)?;
    Ok(RawCompound::Mem(num))
}

fn get_raw_array<'a, const N: usize>(data: &mut &'a [u8]) -> PyResult<RawCompound<'a>> {
    let arr_len = u32::from_be_bytes(split_off_chunk(data)?);
    let byte_len = (arr_len as usize)
        .checked_mul(N)
        .ok_or(PyOverflowError::new_err(
            "Overflow when calculating array length \
            (consider using a 64 bit version of this package)",
        ))?;
    Ok(RawCompound::Mem(split_off(data, byte_len)?))
}

fn get_raw_string<'a>(data: &mut &'a [u8]) -> PyResult<RawCompound<'a>> {
    let length = get_u16(data)? as usize;
    Ok(RawCompound::Mem(split_off(data, length)?))
}

fn get_raw_list<'a>(data: &mut &'a [u8]) -> PyResult<RawCompound<'a>> {
    let tag_id = get_u8(data)?;
    let size = u32::from_be_bytes(split_off_chunk(data)?);
    if tag_id < 7 {
        let tag_size: usize = TAG_SIZE_LUT[tag_id as usize].into();
        let arr_byte_len = tag_size
            .checked_mul(size as usize)
            .ok_or(PyOverflowError::new_err(
                "Overflow when calculating list length \
            (consider using a 64 bit version of this package)",
            ))?;
        return Ok(RawCompound::Mem(split_off(data, arr_byte_len)?));
    }
    let parse_func = TAG_LUT
        .get(tag_id as usize)
        .ok_or_else(|| PyValueError::new_err(format!("Unknown tag id in List: {tag_id}")))?
        .unwrap();
    let mut res = Vec::with_capacity(size as usize);
    for _ in 0..size {
        res.push(parse_func(data)?)
    }

    Ok(RawCompound::List(res))
}

fn get_raw_compound<'a>(data: &mut &'a [u8]) -> PyResult<RawCompound<'a>> {
    let mut map = HashMap::new();
    while let Some(parse_func) = TAG_LUT
        .get(get_u8(data)? as usize)
        .ok_or(PyValueError::new_err("Unknown tag id in Compound"))?
    {
        let name_len = get_u16(data)?;
        let name = split_off(data, name_len.into())?;
        let compound = parse_func(data)?;
        map.insert(name, compound);
    }
    Ok(RawCompound::Map(map))
}

fn load_nbt_raw(data: &'_ [u8]) -> PyResult<RawCompound<'_>> {
    let mut data = data;
    if get_u8(&mut data)? != 10 {
        return Err(PyValueError::new_err("Root tag is not Compound"));
    }
    let name_len = get_u16(&mut data)?;
    let _ = data.split_off(..name_len.into());
    get_raw_compound(&mut data)
}

// Helper Functions

fn split_off<'a>(data: &mut &'a [u8], amount: usize) -> PyResult<&'a [u8]> {
    let name = data
        .split_off(..amount)
        .ok_or(PyEOFError::new_err("Unexpected EOF"))?;
    Ok(name)
}

fn get_u16(data: &mut &[u8]) -> PyResult<u16> {
    Ok(u16::from_be_bytes(split_off_chunk(data)?))
}

fn get_u8(data: &mut &[u8]) -> PyResult<u8> {
    Ok(*data
        .split_off_first()
        .ok_or(PyEOFError::new_err("Unexpected EOF"))?)
}

fn split_off_chunk<const N: usize>(data: &mut &[u8]) -> PyResult<[u8; N]> {
    let res: &[u8; N];
    (res, *data) = data
        .split_first_chunk()
        .ok_or(PyEOFError::new_err("Unexpected EOF"))?;
    Ok(*res)
}

#[pymodule]
mod _core {
    use super::do_compare;
    use pyo3::prelude::*;

    #[pyfunction]
    #[pyo3(signature = (left, right, exclude_last_update = false))]
    fn compare(
        py: Python<'_>,
        left: &[u8],
        right: &[u8],
        exclude_last_update: bool,
    ) -> PyResult<bool> {
        py.detach(|| do_compare(left, right, exclude_last_update))
            .map_err(|(e, side)| {
                e.add_note(py, format!("Occurred while parsing {side}"))
                    .unwrap();
                e
            })
    }
}

fn do_compare(
    left: &[u8],
    right: &[u8],
    exclude_last_update: bool,
) -> Result<bool, (PyErr, &'static str)> {
    let left = load_nbt_raw(left).map_err(|e| (e, "left"))?;
    let right = load_nbt_raw(right).map_err(|e| (e, "right"))?;
    match (exclude_last_update, left, right) {
        (false, left, right) => Ok(left == right),
        (true, RawCompound::Map(mut left), RawCompound::Map(mut right)) => {
            let key = b"LastUpdate".as_slice();
            left.remove(key);
            right.remove(key);
            Ok(left == right)
        }
        _ => unreachable!(),
    }
}
