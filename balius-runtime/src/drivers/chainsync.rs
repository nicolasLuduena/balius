use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use utxorpc::{spec::sync::BlockRef, CardanoSyncClient};

use crate::{Block, ChainPoint, Error, Runtime};

impl From<ChainPoint> for utxorpc::spec::sync::BlockRef {
    fn from(point: ChainPoint) -> Self {
        match point {
            ChainPoint::Cardano(x) => x.clone(),
            #[allow(unreachable_patterns)]
            _ => todo!(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Config {
    pub endpoint_url: String,
    pub headers: Option<HashMap<String, String>>,
}

pub type UndoBlocks = Vec<Block>;
pub type NextBlock = Block;

/// Gather undo blocks from the tip until the next block is encountered.
async fn gather_blocks(
    tip: &mut utxorpc::LiveTip<utxorpc::Cardano>,
) -> Result<(NextBlock, UndoBlocks), Error> {
    let mut undos = vec![];

    loop {
        let event = tip.event().await?;

        match event {
            utxorpc::TipEvent::Apply(chain_block) => {
                let next = Block::Cardano(chain_block.parsed.unwrap());
                break Ok((next, undos));
            }
            utxorpc::TipEvent::Undo(chain_block) => {
                undos.push(Block::Cardano(chain_block.parsed.unwrap()));
            }
            utxorpc::TipEvent::Reset(_) => {
                println!("HOLA");
            }
        }
    }
}

pub async fn run(
    config: Config,
    mut runtime: Runtime,
    cancel: CancellationToken,
) -> Result<(), Error> {
    let mut builder = utxorpc::ClientBuilder::new()
        .uri(&config.endpoint_url)
        .map_err(|e| Error::Driver(e.to_string()))?;

    if let Some(headers) = &config.headers {
        for (k, v) in headers.iter() {
            builder = builder
                .metadata(k, v)
                .map_err(|e| Error::Driver(e.to_string()))?;
        }
    }

    let mut sync = builder.build::<CardanoSyncClient>().await;

    //let cursor = runtime
    //    .chain_cursor()
    //    .await?
    //    .map(Into::into)
    //    .into_iter()
    //    .collect();

    //info!(cursor = ?cursor, "found runtime cursor");

    // TODO: handle disconnections and retry logic

    let mut tip = sync
        .follow_tip(vec![BlockRef {
            hash: hex::decode("9d9e1d2071da5028b6e6a19c6fbb8b6ed3d34af3c25c1a97fb2c1480cead0307")
                .unwrap()
                .into(),
            index: 88366796,
        }])
        .await
        .map_err(|e| Error::Driver(e.to_string()))?;

    // confirm first event is a reset to the requested chain point
    match tip.event().await? {
        utxorpc::TipEvent::Reset(point) => {
            warn!(
                slot = point.index,
                "TODO: check that reset is to the requested chain point"
            );
        }
        _ => return Err(Error::Driver("unexpected event".to_string())),
    }

    info!("starting follow-tip loop");

    loop {
        select! {
            _ = cancel.cancelled() => {
                warn!("chain-sync driver cancelled");
                break Ok(())
            },
            batch = gather_blocks(&mut tip) => {
                let (next, undos) = batch?;
                runtime.handle_chain(&undos, &next).await?;
            }
        }
    }
}
