//! Snake game served over PVAccess.
//!
//! PVs exposed:
//!   SNAKE:DISPLAY    - NTNDArray 20×20 grayscale image (U8, row-major)
//!   SNAKE:UP         - bo  – PUT any value to steer up
//!   SNAKE:DOWN       - bo  – PUT any value to steer down
//!   SNAKE:LEFT       - bo  – PUT any value to steer left
//!   SNAKE:RIGHT      - bo  – PUT any value to steer right
//!   SNAKE:RESET      - bo  – PUT any value to restart immediately
//!   SNAKE:PAUSE      - bo  – PUT 1 to pause ticking, PUT 0 to resume
//!   SNAKE:SCORE      - ai  – current score (food eaten)
//!   SNAKE:HIGHSCORE  - ai  – all-time high score (survives resets)
//!
//! Pixel values in SNAKE:DISPLAY:
//!   0   = empty (black)
//!   100 = snake body
//!   200 = snake head
//!   255 = food
//!
//! The game ticks every 200 ms.  On death the board freezes for ~2 s then
//! auto-resets (or resets immediately on SNAKE:RESET PUT).
//!
//! Run:
//!   cargo run --example snake -p spvirit-server
//!
//! Monitor the display:
//!   cargo run --example pvmonitor -p spvirit-client -- SNAKE:DISPLAY

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use spvirit_server::{
    DbCommonState, OutputMode, PvaServer, RecordData, RecordInstance, RecordType,
};
use spvirit_types::{NdCodec, NdDimension, NtNdArray, NtPayload, ScalarArrayValue, ScalarValue};
use tokio::sync::mpsc;

// ── Grid dimensions ────────────────────────────────────────────────────────

const W: usize = 20;
const H: usize = 20;

// ── Direction / command ────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq)]
enum Dir {
    Up,
    Down,
    Left,
    Right,
}

enum Cmd {
    Turn(Dir),
    Reset,
}

// ── Game state ─────────────────────────────────────────────────────────────

struct Game {
    body: VecDeque<(i32, i32)>,
    dir: Dir,
    next_dir: Dir,
    food: (i32, i32),
    score: u32,
    high_score: u32,
    alive: bool,
    dead_ticks: u32,
    rng: u64,
}

impl Game {
    fn new(seed: u64) -> Self {
        let mut g = Game {
            body: VecDeque::from([(10, 10), (9, 10), (8, 10)]),
            dir: Dir::Right,
            next_dir: Dir::Right,
            food: (0, 0),
            score: 0,
            high_score: 0,
            alive: true,
            dead_ticks: 0,
            rng: seed,
        };
        g.food = g.rand_food();
        g
    }

    fn reset(&mut self) {
        let hs = self.high_score;
        let rng = self.rng.wrapping_add(1);
        *self = Game::new(rng);
        self.high_score = hs;
    }

    // ── LCG PRNG (no external deps) ───────────────────────────────────

    fn rand_next(&mut self) -> u64 {
        self.rng = self
            .rng
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.rng
    }

    fn rand_food(&mut self) -> (i32, i32) {
        for _ in 0..1_000 {
            let x = ((self.rand_next() >> 33) as i32).abs() % W as i32;
            let y = ((self.rand_next() >> 33) as i32).abs() % H as i32;
            if !self.body.contains(&(x, y)) {
                return (x, y);
            }
        }
        (0, 0) // fallback (extremely unlikely)
    }

    // ── Controls ──────────────────────────────────────────────────────

    fn steer(&mut self, d: Dir) {
        let reverse = matches!(
            (self.dir, d),
            (Dir::Up, Dir::Down)
                | (Dir::Down, Dir::Up)
                | (Dir::Left, Dir::Right)
                | (Dir::Right, Dir::Left)
        );
        if !reverse {
            self.next_dir = d;
        }
    }

    // ── Step ──────────────────────────────────────────────────────────

    fn step(&mut self) {
        if !self.alive {
            self.dead_ticks += 1;
            return;
        }

        self.dir = self.next_dir;
        let &(hx, hy) = self.body.front().unwrap();
        let (nx, ny) = match self.dir {
            Dir::Up => (hx, hy - 1),
            Dir::Down => (hx, hy + 1),
            Dir::Left => (hx - 1, hy),
            Dir::Right => (hx + 1, hy),
        };

        // Wall or self collision → die
        if nx < 0 || nx >= W as i32 || ny < 0 || ny >= H as i32 || self.body.contains(&(nx, ny)) {
            self.alive = false;
            return;
        }

        self.body.push_front((nx, ny));

        if (nx, ny) == self.food {
            self.score += 1;
            if self.score > self.high_score {
                self.high_score = self.score;
            }
            self.food = self.rand_food();
        } else {
            self.body.pop_back();
        }
    }

    // ── Render to flat U8 pixel buffer ────────────────────────────────

    fn render(&self) -> Vec<u8> {
        let mut buf = vec![0u8; W * H];

        // Food
        let (fx, fy) = self.food;
        if (0..W as i32).contains(&fx) && (0..H as i32).contains(&fy) {
            buf[fy as usize * W + fx as usize] = 255;
        }

        // Snake (head = 200, body = 100)
        for (i, &(x, y)) in self.body.iter().enumerate() {
            if (0..W as i32).contains(&x) && (0..H as i32).contains(&y) {
                buf[y as usize * W + x as usize] = if i == 0 { 200 } else { 100 };
            }
        }

        // On death: flash the board white every other dead-tick
        if !self.alive && self.dead_ticks % 2 == 1 {
            for v in &mut buf {
                if *v == 0 {
                    *v = 30;
                }
            }
        }

        buf
    }
}

// ── NTNDArray record stub (inserted at startup) ────────────────────────────

fn make_display_record() -> RecordInstance {
    let size = (W * H) as i64;
    RecordInstance {
        name: "SNAKE:DISPLAY".into(),
        record_type: RecordType::NtNdArray,
        common: DbCommonState::default(),
        data: RecordData::NtNdArray {
            nt: NtNdArray {
                value: ScalarArrayValue::U8(vec![0; W * H]),
                codec: NdCodec {
                    name: "none".into(),
                    parameters: HashMap::new(),
                },
                compressed_size: size,
                uncompressed_size: size,
                dimension: vec![
                    NdDimension {
                        size: W as i32,
                        offset: 0,
                        full_size: W as i32,
                        binning: 1,
                        reverse: false,
                    },
                    NdDimension {
                        size: H as i32,
                        offset: 0,
                        full_size: H as i32,
                        binning: 1,
                        reverse: false,
                    },
                ],
                unique_id: 0,
                data_time_stamp: Default::default(),
                attribute: vec![],
                descriptor: Some(format!("Snake game {}×{} display", W, H)),
                alarm: None,
                time_stamp: None,
                display: None,
            },
            inp: None,
            out: None,
            omsl: OutputMode::Supervisory,
        },
        raw_fields: HashMap::new(),
    }
}

// ── Helper to build a frame NtNdArray from a pixel buffer ─────────────────

fn frame_nt(pixels: Vec<u8>, unique_id: i32) -> NtNdArray {
    let size = (W * H) as i64;
    NtNdArray {
        value: ScalarArrayValue::U8(pixels),
        codec: NdCodec {
            name: "none".into(),
            parameters: HashMap::new(),
        },
        compressed_size: size,
        uncompressed_size: size,
        dimension: vec![
            NdDimension {
                size: W as i32,
                offset: 0,
                full_size: W as i32,
                binning: 1,
                reverse: false,
            },
            NdDimension {
                size: H as i32,
                offset: 0,
                full_size: H as i32,
                binning: 1,
                reverse: false,
            },
        ],
        unique_id,
        data_time_stamp: Default::default(),
        attribute: vec![],
        descriptor: Some(format!("Snake game {}×{} display", W, H)),
        alarm: None,
        time_stamp: None,
        display: None,
    }
}

// ── Entry point ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Channel from sync on_put callbacks → async game loop
    let (tx, mut rx) = mpsc::unbounded_channel::<Cmd>();

    let tx_up = tx.clone();
    let tx_down = tx.clone();
    let tx_left = tx.clone();
    let tx_right = tx.clone();
    let tx_reset = tx;

    let server = PvaServer::builder()
        // ── Direction controls ──
        .bo("SNAKE:UP", false)
        .on_put("SNAKE:UP", move |_, _| {
            let _ = tx_up.send(Cmd::Turn(Dir::Up));
        })
        .bo("SNAKE:DOWN", false)
        .on_put("SNAKE:DOWN", move |_, _| {
            let _ = tx_down.send(Cmd::Turn(Dir::Down));
        })
        .bo("SNAKE:LEFT", false)
        .on_put("SNAKE:LEFT", move |_, _| {
            let _ = tx_left.send(Cmd::Turn(Dir::Left));
        })
        .bo("SNAKE:RIGHT", false)
        .on_put("SNAKE:RIGHT", move |_, _| {
            let _ = tx_right.send(Cmd::Turn(Dir::Right));
        })
        // ── Reset / start ──
        .bo("SNAKE:RESET", false)
        .on_put("SNAKE:RESET", move |_, _| {
            let _ = tx_reset.send(Cmd::Reset);
        })
        // ── Pause ──
        .bo("SNAKE:PAUSE", false)
        // ── Score readbacks ──
        .ai("SNAKE:SCORE", 0.0)
        .ai("SNAKE:HIGHSCORE", 0.0)
        .build();

    let store = server.store().clone();

    // Insert the NTNDArray display PV (no builder shortcut for this type yet)
    store
        .insert("SNAKE:DISPLAY".into(), make_display_record())
        .await;

    println!("Snake server running on port 5075");
    println!();
    println!("PVs:");
    println!("  SNAKE:DISPLAY    NTNDArray {}×{} U8 grayscale", W, H);
    println!("  SNAKE:UP/DOWN/LEFT/RIGHT  bo – steer");
    println!("  SNAKE:RESET      bo – restart");
    println!("  SNAKE:PAUSE      bo – 1=pause, 0=resume");
    println!("  SNAKE:SCORE      ai – current score");
    println!("  SNAKE:HIGHSCORE  ai – all-time high");
    println!();
    println!("Pixel values: 0=empty  100=body  200=head  255=food");

    // ── Game loop ──────────────────────────────────────────────────────
    tokio::spawn(async move {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as u64;

        let mut game = Game::new(seed);
        let mut frame: i32 = 0;
        // Auto-reset after this many dead ticks (~2 s at 200 ms/tick)
        const DEAD_RESET_TICKS: u32 = 10;

        let mut interval = tokio::time::interval(Duration::from_millis(200));

        loop {
            interval.tick().await;

            // Drain all pending commands before stepping
            while let Ok(cmd) = rx.try_recv() {
                match cmd {
                    Cmd::Turn(d) => game.steer(d),
                    Cmd::Reset => game.reset(),
                }
            }

            // Check pause PV — skip tick when paused
            let paused = matches!(
                store.get_value("SNAKE:PAUSE").await,
                Some(ScalarValue::Bool(true))
            );
            if paused {
                continue;
            }

            game.step();

            // Auto-reset after a short death pause
            if !game.alive && game.dead_ticks >= DEAD_RESET_TICKS {
                game.reset();
            }

            // Push display
            let pixels = game.render();
            store
                .put_nt("SNAKE:DISPLAY", NtPayload::NdArray(frame_nt(pixels, frame)))
                .await;

            // Push scores
            store
                .set_value("SNAKE:SCORE", ScalarValue::F64(game.score as f64))
                .await;
            store
                .set_value("SNAKE:HIGHSCORE", ScalarValue::F64(game.high_score as f64))
                .await;

            frame = frame.wrapping_add(1);
        }
    });

    server.run().await
}
