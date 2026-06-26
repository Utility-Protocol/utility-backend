use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

pub mod proto {
    tonic::include_proto!("utility.watermark");
}

use proto::offset_reconciliation_server::OffsetReconciliation;
use proto::{ReconciliationRequest, ReconciliationResponse};

pub struct ReconciliationService {
    // In a real implementation, this would have access to the event store
}

impl ReconciliationService {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for ReconciliationService {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl OffsetReconciliation for ReconciliationService {
    type ReconcileStream =
        Pin<Box<dyn Stream<Item = Result<ReconciliationResponse, Status>> + Send>>;

    async fn reconcile(
        &self,
        request: Request<Streaming<ReconciliationRequest>>,
    ) -> Result<Response<Self::ReconcileStream>, Status> {
        let mut in_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(req) => {
                        // In a real implementation:
                        // 1. Fetch event hashes for source_id in [start_offset, end_offset]
                        // 2. Stream them back in chunks

                        // Mock implementation:
                        let response = ReconciliationResponse {
                            source_id: req.source_id,
                            event_hashes: vec![], // Empty for now
                        };
                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = tx.send(Err(err)).await;
                        break;
                    }
                }
            }
        });

        let out_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(out_stream) as Self::ReconcileStream))
    }
}

pub struct ReconciliationClient {
    client:
        proto::offset_reconciliation_client::OffsetReconciliationClient<tonic::transport::Channel>,
}

impl ReconciliationClient {
    pub async fn connect(dst: String) -> Result<Self, tonic::transport::Error> {
        let client =
            proto::offset_reconciliation_client::OffsetReconciliationClient::connect(dst).await?;
        Ok(Self { client })
    }

    pub async fn reconcile(
        &mut self,
        requests: impl Stream<Item = ReconciliationRequest> + Send + 'static,
    ) -> Result<impl Stream<Item = Result<ReconciliationResponse, Status>>, Status> {
        let response = self.client.reconcile(requests).await?;
        Ok(response.into_inner())
    }
}
