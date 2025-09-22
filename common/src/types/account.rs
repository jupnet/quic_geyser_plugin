use jupnet_sdk::{account::Account as JupnetAccount, pubkey::Pubkey};
use serde::{Deserialize, Serialize};

use crate::compression::CompressionType;

use super::slot_identifier::SlotIdentifier;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct Account {
    pub slot_identifier: SlotIdentifier,
    pub pubkey: Pubkey,
    pub owner: Pubkey,
    pub lamports: u64,
    pub executable: bool,
    pub rent_epoch: u64,
    pub write_version: u64,
    pub data: Vec<u8>,
    pub compression_type: CompressionType,
    pub data_length: u64,
}

impl Account {
    pub fn new(
        pubkey: Pubkey,
        jupnet_account: JupnetAccount,
        compression_type: CompressionType,
        slot_identifier: SlotIdentifier,
        write_version: u64,
    ) -> Self {
        let data_length = jupnet_account.data.len() as u64;

        let data = if !jupnet_account.data.is_empty() {
            match compression_type {
                CompressionType::None => jupnet_account.data,
                CompressionType::Lz4Fast(speed) => lz4::block::compress(
                    &jupnet_account.data,
                    Some(lz4::block::CompressionMode::FAST(speed)),
                    true,
                )
                .expect("Compression should work"),
                CompressionType::Lz4(compression) => lz4::block::compress(
                    &jupnet_account.data,
                    Some(lz4::block::CompressionMode::HIGHCOMPRESSION(compression)),
                    true,
                )
                .expect("compression should work"),
            }
        } else {
            vec![]
        };
        Account {
            slot_identifier,
            pubkey,
            owner: jupnet_account.owner,
            write_version,
            data,
            compression_type,
            data_length,
            lamports: jupnet_account.lamports,
            executable: jupnet_account.executable,
            rent_epoch: jupnet_account.rent_epoch,
        }
    }

    pub fn jupnet_account(&self) -> JupnetAccount {
        match self.compression_type {
            CompressionType::None => JupnetAccount {
                lamports: self.lamports,
                data: self.data.clone(),
                owner: self.owner,
                executable: self.executable,
                rent_epoch: self.rent_epoch,
            },
            CompressionType::Lz4(_) | CompressionType::Lz4Fast(_) => {
                let uncompressed_data = if self.data_length > 0 {
                    lz4::block::decompress(&self.data, None).expect("should uncompress")
                } else {
                    vec![]
                };

                JupnetAccount {
                    lamports: self.lamports,
                    data: uncompressed_data,
                    owner: self.owner,
                    executable: self.executable,
                    rent_epoch: self.rent_epoch,
                }
            }
        }
    }
}
