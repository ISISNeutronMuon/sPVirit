use spvirit_codec::{PvaPacket, PvaPacketCommand};

fn main() {
    // A minimal PVA Connection Validation packet (little-endian):
    //   Header  (8 bytes): magic=0xCA, version=2, flags=0x00 (LE, app, client), command=1
    //   Payload (9 bytes): buffer_size=16384(u32) + introspection_registry_size=0x7fff(u16) + qos=0(u16) + authz=""
    let raw: &[u8] = &[
        0xCA, 0x02, 0x00, 0x01, // header: magic, version, flags (LE), command 1
        0x09, 0x00, 0x00, 0x00, // payload length = 9
        0x00, 0x40, 0x00, 0x00, // buffer_size = 16384
        0xFF, 0x7F, // introspection_registry_size = 32767
        0x00, 0x00, // qos = 0
        0x00, // authz = "" (empty string, size=0)
    ];

    let mut packet = PvaPacket::new(raw);

    println!("Header:");
    println!("  magic:   0x{:02X}", packet.header.magic);
    println!("  version: {}", packet.header.version);
    println!("  command: {}", packet.header.command);
    println!("  payload: {} bytes", packet.header.payload_length);
    println!(
        "  endian:  {}",
        if packet.header.flags.is_msb {
            "big"
        } else {
            "little"
        }
    );

    if let Some(cmd) = packet.decode_payload() {
        match cmd {
            PvaPacketCommand::ConnectionValidation(cv) => {
                println!("\nConnectionValidation:");
                println!("  buffer_size:  {}", cv.buffer_size);
                println!("  intro_reg:    {}", cv.introspection_registry_size);
                println!("  qos:          {}", cv.qos);
                println!("  authz:        {:?}", cv.authz);
            }
            other => println!("Decoded: {other:?}"),
        }
    } else {
        eprintln!("Failed to decode payload");
    }
}
