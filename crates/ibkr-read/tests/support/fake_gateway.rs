use std::io::{self, ErrorKind};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const SERVER_VERSION: i32 = 215;
const REQUEST_CONTRACT_DATA: i32 = 9;
pub(crate) const CANCEL_CONTRACT_DATA: i32 = 106;

#[derive(Clone, Debug)]
pub(crate) struct ContractFixture {
    pub(crate) contract_id: i32,
    pub(crate) local_symbol: String,
    pub(crate) exchange: String,
    pub(crate) currency: String,
    pub(crate) multiplier: f64,
}

impl ContractFixture {
    pub(crate) fn exact(contract_id: i32) -> Self {
        Self {
            contract_id,
            local_symbol: "AAPL  270115C00150000".to_string(),
            exchange: "CBOE".to_string(),
            currency: "USD".to_string(),
            multiplier: 100.0,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum ResponsePlan {
    RowsAndEnd(Vec<ContractFixture>),
    RowsThenEndAfterCancel(Vec<ContractFixture>),
    RowsWithoutEnd(Vec<ContractFixture>),
    CloseAfterRows(Vec<ContractFixture>),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Observation {
    pub(crate) outbound_ids: Vec<i32>,
    pub(crate) search: Option<SearchRequest>,
}

#[derive(Clone, Debug)]
pub(crate) struct SearchRequest {
    pub(crate) request_id: i32,
    pub(crate) contract_id: i32,
    pub(crate) symbol: String,
    pub(crate) security_type: String,
    pub(crate) expiration: String,
    pub(crate) strike: f64,
    pub(crate) right: String,
    pub(crate) exchange: String,
    pub(crate) currency: String,
    pub(crate) local_symbol: String,
}

pub(crate) struct FakeGateway {
    pub(crate) address: std::net::SocketAddr,
    observation: Arc<Mutex<Observation>>,
    task: JoinHandle<io::Result<()>>,
}

impl FakeGateway {
    pub(crate) async fn start(plan: ResponsePlan) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let observation = Arc::new(Mutex::new(Observation::default()));
        let observed = Arc::clone(&observation);
        let task = tokio::spawn(async move { serve(listener, plan, observed).await });
        Ok(Self {
            address,
            observation,
            task,
        })
    }

    pub(crate) fn observation(&self) -> Observation {
        self.observation
            .lock()
            .expect("synthetic gateway observation mutex")
            .clone()
    }

    pub(crate) async fn finish(self) -> io::Result<Observation> {
        let observation = Arc::clone(&self.observation);
        tokio::time::timeout(std::time::Duration::from_secs(2), self.task)
            .await
            .map_err(|_| io::Error::new(ErrorKind::TimedOut, "synthetic gateway did not close"))?
            .map_err(|error| {
                io::Error::other(format!("synthetic gateway task failed: {error}"))
            })??;
        let snapshot = observation
            .lock()
            .expect("synthetic gateway observation mutex")
            .clone();
        Ok(snapshot)
    }
}

async fn serve(
    listener: TcpListener,
    plan: ResponsePlan,
    observation: Arc<Mutex<Observation>>,
) -> io::Result<()> {
    let (mut stream, _) = listener.accept().await?;
    read_client_handshake(&mut stream).await?;
    send_handshake(&mut stream).await?;

    let request = loop {
        let Some(packet) = read_packet(&mut stream).await? else {
            return Ok(());
        };
        let Some(message_id) = decode_message_id(&packet) else {
            continue;
        };
        observation
            .lock()
            .expect("synthetic gateway observation mutex")
            .outbound_ids
            .push(message_id);
        if message_id == REQUEST_CONTRACT_DATA {
            let request = decode_search_request(&packet)?;
            observation
                .lock()
                .expect("synthetic gateway observation mutex")
                .search = Some(request.clone());
            break request;
        }
    };

    match plan {
        ResponsePlan::RowsAndEnd(rows) => {
            send_rows(&mut stream, request.request_id, &rows).await?;
            write_protocol_packet(
                &mut stream,
                protocol_frame(
                    52,
                    &varint_field(
                        1,
                        u64::try_from(request.request_id).expect("positive synthetic request id"),
                    ),
                ),
            )
            .await?;
        }
        ResponsePlan::RowsWithoutEnd(rows) => {
            send_rows(&mut stream, request.request_id, &rows).await?
        }
        ResponsePlan::RowsThenEndAfterCancel(rows) => {
            send_rows(&mut stream, request.request_id, &rows).await?;
            loop {
                let Some(packet) = read_packet(&mut stream).await? else {
                    return Ok(());
                };
                let Some(message_id) = decode_message_id(&packet) else {
                    continue;
                };
                observation
                    .lock()
                    .expect("synthetic gateway observation mutex")
                    .outbound_ids
                    .push(message_id);
                if message_id == CANCEL_CONTRACT_DATA {
                    write_protocol_packet(
                        &mut stream,
                        protocol_frame(
                            52,
                            &varint_field(
                                1,
                                u64::try_from(request.request_id)
                                    .expect("positive synthetic request id"),
                            ),
                        ),
                    )
                    .await?;
                    break;
                }
            }
        }
        ResponsePlan::CloseAfterRows(rows) => {
            send_rows(&mut stream, request.request_id, &rows).await?;
            return Ok(());
        }
    }

    while let Some(packet) = read_packet(&mut stream).await? {
        if let Some(message_id) = decode_message_id(&packet) {
            observation
                .lock()
                .expect("synthetic gateway observation mutex")
                .outbound_ids
                .push(message_id);
        }
    }
    Ok(())
}

async fn read_client_handshake(stream: &mut TcpStream) -> io::Result<()> {
    let mut magic = [0u8; 4];
    stream.read_exact(&mut magic).await?;
    if &magic != b"API\0" {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "unexpected synthetic API preamble",
        ));
    }
    let mut length = [0u8; 4];
    stream.read_exact(&mut length).await?;
    let length =
        usize::try_from(u32::from_be_bytes(length)).expect("u32 fits usize on supported targets");
    if length > 256 {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "oversized synthetic version range",
        ));
    }
    let mut version_range = vec![0; length];
    stream.read_exact(&mut version_range).await?;
    Ok(())
}

async fn send_handshake(stream: &mut TcpStream) -> io::Result<()> {
    let mut greeting = format!("{SERVER_VERSION}\0").into_bytes();
    greeting.extend_from_slice(b"20261008 00:00:00 UTC\0");
    write_protocol_packet(stream, &greeting).await?;
    write_protocol_packet(stream, &protocol_frame(9, &varint_field(1, 9_000))).await?;
    write_protocol_packet(
        stream,
        &protocol_frame(15, &bytes_field(1, b"SYNTHETIC_ACCOUNT")),
    )
    .await?;
    Ok(())
}

async fn send_rows(
    stream: &mut TcpStream,
    request_id: i32,
    rows: &[ContractFixture],
) -> io::Result<()> {
    for row in rows {
        let contract = encode_contract(row);
        let mut body = varint_field(
            1,
            u64::try_from(request_id).expect("positive synthetic request id"),
        );
        body.extend(bytes_field(2, &contract));
        body.extend(bytes_field(3, &[]));
        write_protocol_packet(stream, &protocol_frame(10, &body)).await?;
    }
    Ok(())
}

fn encode_contract(row: &ContractFixture) -> Vec<u8> {
    let mut body = varint_field(
        1,
        u64::try_from(row.contract_id).expect("positive synthetic contract id"),
    );
    body.extend(bytes_field(2, b"AAPL"));
    body.extend(bytes_field(3, b"OPT"));
    body.extend(bytes_field(4, b"20270115"));
    body.extend(fixed64_field(5, 150.0_f64.to_le_bytes()));
    body.extend(bytes_field(6, b"C"));
    body.extend(fixed64_field(7, row.multiplier.to_le_bytes()));
    body.extend(bytes_field(8, row.exchange.as_bytes()));
    body.extend(bytes_field(10, row.currency.as_bytes()));
    body.extend(bytes_field(11, row.local_symbol.as_bytes()));
    body.extend(bytes_field(12, b"AAPL"));
    body
}

fn decode_search_request(packet: &[u8]) -> io::Result<SearchRequest> {
    let body = packet
        .get(4..)
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "short protocol packet"))?;
    let fields = decode_fields(body)?;
    let request_id = i32::try_from(required_varint(&fields, 1)?)
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "request id overflow"))?;
    let contract = required_bytes(&fields, 2)?;
    let contract_fields = decode_fields(contract)?;
    Ok(SearchRequest {
        request_id,
        contract_id: i32::try_from(required_varint(&contract_fields, 1)?)
            .map_err(|_| io::Error::new(ErrorKind::InvalidData, "contract id overflow"))?,
        symbol: required_string(&contract_fields, 2)?,
        security_type: required_string(&contract_fields, 3)?,
        expiration: required_string(&contract_fields, 4)?,
        strike: f64::from_le_bytes(required_fixed64(&contract_fields, 5)?),
        right: required_string(&contract_fields, 6)?,
        exchange: required_string(&contract_fields, 8)?,
        currency: required_string(&contract_fields, 10)?,
        local_symbol: required_string(&contract_fields, 11)?,
    })
}

#[derive(Clone, Copy, Debug)]
enum Field<'a> {
    Varint(u64),
    Fixed64([u8; 8]),
    Bytes(&'a [u8]),
}

fn decode_fields(mut input: &[u8]) -> io::Result<Vec<(u32, Field<'_>)>> {
    let mut fields = Vec::new();
    while !input.is_empty() {
        let key = read_varint(&mut input)?;
        let number = u32::try_from(key >> 3)
            .map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid field number"))?;
        let wire = key & 7;
        let value = match wire {
            0 => Field::Varint(read_varint(&mut input)?),
            1 => {
                let bytes = take(&mut input, 8)?;
                Field::Fixed64(bytes.try_into().expect("8-byte fixed64"))
            }
            2 => {
                let length = usize::try_from(read_varint(&mut input)?)
                    .map_err(|_| io::Error::new(ErrorKind::InvalidData, "length overflow"))?;
                Field::Bytes(take(&mut input, length)?)
            }
            5 => {
                take(&mut input, 4)?;
                continue;
            }
            _ => {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "unsupported protobuf wire type",
                ));
            }
        };
        fields.push((number, value));
    }
    Ok(fields)
}

fn required_varint(fields: &[(u32, Field<'_>)], number: u32) -> io::Result<u64> {
    fields
        .iter()
        .find_map(|(field, value)| match (*field, value) {
            (found, Field::Varint(value)) if found == number => Some(*value),
            _ => None,
        })
        .ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("missing varint field {number}"),
            )
        })
}

fn required_bytes<'a>(fields: &'a [(u32, Field<'a>)], number: u32) -> io::Result<&'a [u8]> {
    fields
        .iter()
        .find_map(|(field, value)| match (*field, value) {
            (found, Field::Bytes(value)) if found == number => Some(*value),
            _ => None,
        })
        .ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("missing bytes field {number}"),
            )
        })
}

fn required_string(fields: &[(u32, Field<'_>)], number: u32) -> io::Result<String> {
    String::from_utf8(required_bytes(fields, number)?.to_vec()).map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("field {number} is not UTF-8"),
        )
    })
}

fn required_fixed64(fields: &[(u32, Field<'_>)], number: u32) -> io::Result<[u8; 8]> {
    fields
        .iter()
        .find_map(|(field, value)| match (*field, value) {
            (found, Field::Fixed64(value)) if found == number => Some(*value),
            _ => None,
        })
        .ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("missing fixed64 field {number}"),
            )
        })
}

fn read_varint(input: &mut &[u8]) -> io::Result<u64> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let byte = u64::from(*take(input, 1)?.first().expect("one byte"));
        if shift == 63 && byte > 1 {
            return Err(io::Error::new(ErrorKind::InvalidData, "varint overflow"));
        }
        value |= (byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(io::Error::new(
        ErrorKind::InvalidData,
        "unterminated varint",
    ))
}

fn take<'a>(input: &mut &'a [u8], length: usize) -> io::Result<&'a [u8]> {
    if length > input.len() {
        return Err(io::Error::new(
            ErrorKind::UnexpectedEof,
            "truncated protobuf field",
        ));
    }
    let (head, rest) = input.split_at(length);
    *input = rest;
    Ok(head)
}

fn varint_field(number: u32, value: u64) -> Vec<u8> {
    let mut output = encode_varint(u64::from(number) << 3);
    output.extend(encode_varint(value));
    output
}

fn bytes_field(number: u32, value: &[u8]) -> Vec<u8> {
    let mut output = encode_varint((u64::from(number) << 3) | 2);
    output.extend(encode_varint(
        u64::try_from(value.len()).expect("test protocol payload fits u64"),
    ));
    output.extend_from_slice(value);
    output
}

fn fixed64_field(number: u32, value: [u8; 8]) -> Vec<u8> {
    let mut output = encode_varint((u64::from(number) << 3) | 1);
    output.extend(value);
    output
}

fn encode_varint(mut value: u64) -> Vec<u8> {
    let mut output = Vec::new();
    while value >= 0x80 {
        output.push(u8::try_from(value & 0x7f).expect("masked varint byte") | 0x80);
        value >>= 7;
    }
    output.push(u8::try_from(value).expect("terminal varint byte"));
    output
}

fn protocol_frame(message_id: i32, protobuf: &[u8]) -> Vec<u8> {
    let mut message = (message_id + 200).to_be_bytes().to_vec();
    message.extend_from_slice(protobuf);
    message
}

fn decode_message_id(packet: &[u8]) -> Option<i32> {
    let encoded = i32::from_be_bytes(packet.get(..4)?.try_into().ok()?);
    (encoded >= 200).then_some(encoded - 200)
}

async fn write_protocol_packet(stream: &mut TcpStream, body: &[u8]) -> io::Result<()> {
    let length = u32::try_from(body.len())
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "packet too large"))?;
    stream.write_all(&length.to_be_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

async fn read_packet(stream: &mut TcpStream) -> io::Result<Option<Vec<u8>>> {
    let mut length = [0u8; 4];
    match stream.read_exact(&mut length).await {
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let length =
        usize::try_from(u32::from_be_bytes(length)).expect("u32 fits usize on supported targets");
    if !(4..=64 * 1024).contains(&length) {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "invalid synthetic packet size",
        ));
    }
    let mut packet = vec![0; length];
    stream.read_exact(&mut packet).await?;
    Ok(Some(packet))
}
