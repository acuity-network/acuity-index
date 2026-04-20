#![cfg_attr(not(feature = "std"), no_std)]

pub use pallet::*;

use frame_support::weights::Weight;

pub trait WeightInfo {
    fn store_record() -> Weight;
    fn store_links() -> Weight;
    fn emit_burst() -> Weight;
}

impl WeightInfo for () {
    fn store_record() -> Weight {
        Weight::from_parts(100_000, 0)
    }

    fn store_links() -> Weight {
        Weight::from_parts(100_000, 0)
    }

    fn emit_burst() -> Weight {
        Weight::from_parts(200_000, 0)
    }
}

#[frame_support::pallet]
pub mod pallet {
    use super::*;
    use frame_support::pallet_prelude::*;
    use frame_system::pallet_prelude::*;

    #[pallet::pallet]
    pub struct Pallet<T>(_);

    #[pallet::config]
    pub trait Config: frame_system::Config {
        #[allow(deprecated)]
        type RuntimeEvent: From<Event<Self>> + IsType<<Self as frame_system::Config>::RuntimeEvent>;
        type WeightInfo: WeightInfo;
        #[pallet::constant]
        type MaxBurstCount: Get<u32>;
    }

    #[pallet::event]
    #[pallet::generate_deposit(pub(super) fn deposit_event)]
    pub enum Event<T: Config> {
        RecordStored {
            record_id: u32,
            owner: T::AccountId,
            digest: [u8; 32],
            topics: [u32; 2],
        },
        LinksStored {
            record_id: u32,
            related_ids: [u32; 2],
            related_digests: [[u8; 32]; 2],
        },
        BurstEmitted {
            batch_id: u32,
            seq: u32,
            owner: T::AccountId,
            digest: [u8; 32],
        },
    }

    #[pallet::error]
    pub enum Error<T> {
        TooManyBurstEvents,
    }

    #[pallet::call]
    impl<T: Config> Pallet<T> {
        #[pallet::call_index(0)]
        #[pallet::weight(T::WeightInfo::store_record())]
        pub fn store_record(
            origin: OriginFor<T>,
            record_id: u32,
            digest: [u8; 32],
            topics: [u32; 2],
        ) -> DispatchResult {
            let owner = ensure_signed(origin)?;
            Self::deposit_event(Event::RecordStored {
                record_id,
                owner,
                digest,
                topics,
            });
            Ok(())
        }

        #[pallet::call_index(1)]
        #[pallet::weight(T::WeightInfo::store_links())]
        pub fn store_links(
            origin: OriginFor<T>,
            record_id: u32,
            related_ids: [u32; 2],
            related_digests: [[u8; 32]; 2],
        ) -> DispatchResult {
            let _ = ensure_signed(origin)?;
            Self::deposit_event(Event::LinksStored {
                record_id,
                related_ids,
                related_digests,
            });
            Ok(())
        }

        #[pallet::call_index(2)]
        #[pallet::weight(T::WeightInfo::emit_burst())]
        pub fn emit_burst(origin: OriginFor<T>, batch_id: u32, count: u32) -> DispatchResult {
            let owner = ensure_signed(origin)?;
            ensure!(
                count <= T::MaxBurstCount::get(),
                Error::<T>::TooManyBurstEvents
            );

            for seq in 0..count {
                Self::deposit_event(Event::BurstEmitted {
                    batch_id,
                    seq,
                    owner: owner.clone(),
                    digest: synthetic_digest(batch_id, seq),
                });
            }

            Ok(())
        }
    }

    fn synthetic_digest(batch_id: u32, seq: u32) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        bytes[..4].copy_from_slice(&batch_id.to_be_bytes());
        bytes[4..8].copy_from_slice(&seq.to_be_bytes());
        for (idx, byte) in bytes[8..].iter_mut().enumerate() {
            *byte = batch_id
                .wrapping_mul(31)
                .wrapping_add(seq.wrapping_mul(17))
                .wrapping_add(idx as u32) as u8;
        }
        bytes
    }
}
