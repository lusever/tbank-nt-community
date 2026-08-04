pub mod google {
    pub mod api {
        include!(concat!(env!("OUT_DIR"), "/google.api.rs"));
    }
}

pub mod tinkoff {
    pub mod public {
        pub mod invest {
            pub mod api {
                pub mod contract {
                    pub mod v1 {
                        // Generated from the vendored upstream protobuf contracts.
                        #![allow(clippy::large_enum_variant, clippy::tabs_in_doc_comments)]
                        include!(concat!(
                            env!("OUT_DIR"),
                            "/tinkoff.public.invest.api.contract.v1.rs"
                        ));
                    }
                }
            }
        }
    }
}

pub use tinkoff::public::invest::api::contract::v1::*;
