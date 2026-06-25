#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CommodityType {
    Electricity = 0,
    Gas = 1,
    Water = 2,
    Heat = 3,
    Other = 255,
}

impl CommodityType {
    pub fn scale_byte(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for CommodityType {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Electricity),
            1 => Ok(Self::Gas),
            2 => Ok(Self::Water),
            3 => Ok(Self::Heat),
            255 => Ok(Self::Other),
            _ => Err("unknown commodity type"),
        }
    }
}
