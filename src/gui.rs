use eframe::egui::{self, Color32, RichText, ScrollArea, Slider, TextEdit, Vec2};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc};

use crate::db::{self, DupeGroup, SimilarFile, Stats};
use crate::scan::{self, ScanProgress, ScanResult};

// tree_magic_mini is a direct dep; import it only for run_find
use tree_magic_mini;

// ── Tab ──────────────────────────────────────────────────────────────────────

#[derive(PartialEq, Clone, Copy)]
enum Tab {
    Scan,
    Dupes,
    Find,
    Stats,
    Stale,
}

// ── Scan panel ───────────────────────────────────────────────────────────────

enum ScanStatus {
    Idle,
    Running {
        progress: Arc<ScanProgress>,
        result_rx: mpsc::Receiver<anyhow::Result<ScanResult>>,
    },
    Done(ScanResult),
    Error(String),
}

struct ScanPanel {
    scan_path: String,
    threads: usize,
    status: ScanStatus,
    dir_dialog_rx: Option<mpsc::Receiver<Option<PathBuf>>>,
}

impl ScanPanel {
    fn new() -> Self {
        let threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            scan_path: String::new(),
            threads,
            status: ScanStatus::Idle,
            dir_dialog_rx: None,
        }
    }
}

// ── Dupes panel ──────────────────────────────────────────────────────────────

enum DupesStatus {
    Idle,
    Loading(mpsc::Receiver<anyhow::Result<Vec<DupeGroup>>>),
    Done(Vec<DupeGroup>),
    Error(String),
}

struct DupesPanel {
    min_size_str: String,
    status: DupesStatus,
    selected_group: Option<usize>,
}

impl DupesPanel {
    fn new() -> Self {
        Self { min_size_str: "1".to_string(), status: DupesStatus::Idle, selected_group: None }
    }
}

// ── Find panel ───────────────────────────────────────────────────────────────

struct FindResult {
    exact: Vec<String>,
    similar: Vec<SimilarFile>,
    sha256: Option<String>,
}

enum FindStatus {
    Idle,
    Loading(mpsc::Receiver<anyhow::Result<FindResult>>),
    Done(FindResult),
    Error(String),
}

struct FindPanel {
    input: String,
    threshold: u32,
    status: FindStatus,
    selected_path: Option<String>,
    preview_rx: Option<mpsc::Receiver<egui::ColorImage>>,
    preview_texture: Option<egui::TextureHandle>,
    file_dialog_rx: Option<mpsc::Receiver<Option<PathBuf>>>,
}

impl FindPanel {
    fn new() -> Self {
        Self {
            input: String::new(),
            threshold: 31,
            status: FindStatus::Idle,
            selected_path: None,
            preview_rx: None,
            preview_texture: None,
            file_dialog_rx: None,
        }
    }
}

// ── Stats panel ──────────────────────────────────────────────────────────────

enum StatsStatus {
    Idle,
    Loading(mpsc::Receiver<anyhow::Result<Stats>>),
    Done(Stats),
    Error(String),
}

struct StatsPanel {
    status: StatsStatus,
}

impl StatsPanel {
    fn new() -> Self {
        Self { status: StatsStatus::Idle }
    }
}

// ── Stale panel ──────────────────────────────────────────────────────────────

enum StaleStatus {
    Idle,
    Loading(mpsc::Receiver<anyhow::Result<Vec<String>>>),
    Done(Vec<String>),
    Error(String),
}

struct StalePanel {
    status: StaleStatus,
}

impl StalePanel {
    fn new() -> Self {
        Self { status: StaleStatus::Idle }
    }
}

// ── App ──────────────────────────────────────────────────────────────────────

pub struct HashGoblinApp {
    db_path: String,
    db_dialog_rx: Option<mpsc::Receiver<Option<PathBuf>>>,
    active_tab: Tab,
    scan: ScanPanel,
    dupes: DupesPanel,
    find: FindPanel,
    stats: StatsPanel,
    stale: StalePanel,
}

impl HashGoblinApp {
    fn new() -> Self {
        Self {
            db_path: "hashgoblin.db".to_string(),
            db_dialog_rx: None,
            active_tab: Tab::Scan,
            scan: ScanPanel::new(),
            dupes: DupesPanel::new(),
            find: FindPanel::new(),
            stats: StatsPanel::new(),
            stale: StalePanel::new(),
        }
    }
}

pub fn run() -> anyhow::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("HashGoblin")
            .with_inner_size([1100.0, 720.0]),
        ..Default::default()
    };
    eframe::run_native(
        "HashGoblin",
        options,
        Box::new(|_cc| Ok(Box::new(HashGoblinApp::new()))),
    )
    .map_err(|e| anyhow::anyhow!("{}", e))
}

// ── eframe::App ──────────────────────────────────────────────────────────────

impl eframe::App for HashGoblinApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Keep repainting while background work is in flight.
        let busy = matches!(self.scan.status, ScanStatus::Running { .. })
            || matches!(self.dupes.status, DupesStatus::Loading(_))
            || matches!(self.find.status, FindStatus::Loading(_))
            || matches!(self.stats.status, StatsStatus::Loading(_))
            || matches!(self.stale.status, StaleStatus::Loading(_))
            || self.find.preview_rx.is_some();
        if busy {
            ctx.request_repaint_after(std::time::Duration::from_millis(80));
        }

        // ── DB bar ────────────────────────────────────────────────────────
        egui::TopBottomPanel::top("db_bar").show(ctx, |ui| {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                ui.label("Database:");
                ui.add(
                    TextEdit::singleline(&mut self.db_path)
                        .desired_width(440.0)
                        .hint_text("hashgoblin.db"),
                );
                if ui.button("Browse…").clicked() {
                    let (tx, rx) = mpsc::channel();
                    let current = self.db_path.clone();
                    std::thread::spawn(move || {
                        let path = rfd::FileDialog::new()
                            .set_title("Select or create database")
                            .add_filter("SQLite", &["db", "sqlite", "sqlite3"])
                            .set_file_name(&current)
                            .save_file();
                        tx.send(path).ok();
                    });
                    self.db_dialog_rx = Some(rx);
                }
            });
            ui.add_space(4.0);
        });

        // Poll DB file dialog
        if let Some(ref rx) = self.db_dialog_rx {
            if let Ok(opt) = rx.try_recv() {
                if let Some(p) = opt {
                    self.db_path = p.to_string_lossy().to_string();
                }
                self.db_dialog_rx = None;
            }
        }

        // ── Tab bar ───────────────────────────────────────────────────────
        egui::TopBottomPanel::top("tabs").show(ctx, |ui| {
            ui.horizontal(|ui| {
                for (tab, label) in [
                    (Tab::Scan, "Scan"),
                    (Tab::Dupes, "Dupes"),
                    (Tab::Find, "Find"),
                    (Tab::Stats, "Stats"),
                    (Tab::Stale, "Stale"),
                ] {
                    let sel = self.active_tab == tab;
                    if ui.selectable_label(sel, label).clicked() {
                        self.active_tab = tab;
                    }
                }
            });
        });

        // ── Content ───────────────────────────────────────────────────────
        let db = self.db_path.clone();
        egui::CentralPanel::default().show(ctx, |ui| match self.active_tab {
            Tab::Scan => show_scan(ui, ctx, &db, &mut self.scan),
            Tab::Dupes => show_dupes(ui, &db, &mut self.dupes),
            Tab::Find => show_find(ui, ctx, &db, &mut self.find),
            Tab::Stats => show_stats(ui, &db, &mut self.stats),
            Tab::Stale => show_stale(ui, &db, &mut self.stale),
        });
    }
}

// ── Scan panel ───────────────────────────────────────────────────────────────

fn show_scan(ui: &mut egui::Ui, ctx: &egui::Context, db: &str, p: &mut ScanPanel) {
    // Poll file dialog
    if let Some(ref rx) = p.dir_dialog_rx {
        if let Ok(opt) = rx.try_recv() {
            if let Some(path) = opt {
                p.scan_path = path.to_string_lossy().to_string();
            }
            p.dir_dialog_rx = None;
        }
    }

    // Poll for scan completion
    let mut finished: Option<anyhow::Result<ScanResult>> = None;
    if let ScanStatus::Running { ref result_rx, .. } = p.status {
        if let Ok(r) = result_rx.try_recv() {
            finished = Some(r);
        }
    }
    if let Some(r) = finished {
        p.status = match r {
            Ok(res) => ScanStatus::Done(res),
            Err(e) => ScanStatus::Error(e.to_string()),
        };
    }

    ui.add_space(8.0);
    ui.heading("Scan Directory");
    ui.add_space(6.0);

    let running = matches!(p.status, ScanStatus::Running { .. });

    ui.horizontal(|ui| {
        ui.label("Directory:");
        ui.add_enabled(
            !running,
            TextEdit::singleline(&mut p.scan_path)
                .desired_width(420.0)
                .hint_text("/path/to/scan"),
        );
        if ui.add_enabled(!running, egui::Button::new("Browse…")).clicked() {
            let (tx, rx) = mpsc::channel();
            std::thread::spawn(move || {
                let path = rfd::FileDialog::new()
                    .set_title("Select directory to scan")
                    .pick_folder();
                tx.send(path).ok();
            });
            p.dir_dialog_rx = Some(rx);
        }
    });

    ui.horizontal(|ui| {
        ui.label("Threads:");
        ui.add_enabled(
            !running,
            egui::DragValue::new(&mut p.threads).range(1..=256),
        );
    });

    ui.add_space(6.0);
    ui.horizontal(|ui| {
        let can_start = !running && !p.scan_path.is_empty();
        if ui.add_enabled(can_start, egui::Button::new("▶  Start Scan")).clicked() {
            let scan_path = p.scan_path.clone();
            let db_path = db.to_string();
            let threads = p.threads;
            let progress = ScanProgress::new();
            let (tx, rx) = mpsc::channel();
            let prog_clone = progress.clone();
            std::thread::spawn(move || {
                let result = (|| -> anyhow::Result<ScanResult> {
                    let t = scan::now_unix()?;
                    scan::run(
                        std::path::Path::new(&scan_path),
                        std::path::Path::new(&db_path),
                        threads,
                        t,
                        Some(prog_clone),
                    )
                })();
                tx.send(result).ok();
            });
            p.status = ScanStatus::Running { progress, result_rx: rx };
        }

        if running {
            if let ScanStatus::Running { ref progress, .. } = p.status {
                if ui.button("✖  Cancel").clicked() {
                    progress.cancel.store(true, Ordering::Relaxed);
                }
            }
        }
    });

    ui.add_space(10.0);
    ui.separator();
    ui.add_space(6.0);

    match &p.status {
        ScanStatus::Idle => {
            ui.label(RichText::new("Ready.").color(Color32::GRAY));
        }
        ScanStatus::Running { progress, .. } => {
            ctx.request_repaint_after(std::time::Duration::from_millis(80));
            let phase = progress.phase.lock().unwrap().clone();
            match phase.as_str() {
                "walking" => {
                    let found = progress.walk_count.load(Ordering::Relaxed);
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label(format!("Walking… {} files found", found));
                    });
                }
                _ => {
                    let total = progress.total_to_hash.load(Ordering::Relaxed);
                    let done = progress.hashed_count.load(Ordering::Relaxed);
                    let cur = progress.current_file.lock().unwrap().clone();

                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label(format!("Hashing: {}/{}", done, total));
                    });
                    if total > 0 {
                        let frac = done as f32 / total as f32;
                        ui.add(
                            egui::ProgressBar::new(frac)
                                .desired_width(500.0)
                                .show_percentage(),
                        );
                    }
                    if !cur.is_empty() {
                        ui.label(
                            RichText::new(truncate_path(&cur, 80))
                                .color(Color32::GRAY)
                                .monospace(),
                        );
                    }
                }
            }
        }
        ScanStatus::Done(res) => {
            ui.label(
                RichText::new("✔  Scan complete")
                    .color(Color32::from_rgb(80, 200, 80))
                    .strong(),
            );
            ui.add_space(4.0);
            egui::Grid::new("scan_results").num_columns(2).spacing([20.0, 4.0]).show(ui, |ui| {
                stat_row(ui, "Hashed", res.processed);
                stat_row(ui, "Skipped (unchanged)", res.skipped);
                stat_row(ui, "Errors", res.errors);
                stat_row(ui, "Stale (marked missing)", res.stale);
            });
        }
        ScanStatus::Error(e) => {
            ui.label(RichText::new(format!("✖  Error: {}", e)).color(Color32::RED));
        }
    }
}

// ── Dupes panel ──────────────────────────────────────────────────────────────

fn show_dupes(ui: &mut egui::Ui, db: &str, p: &mut DupesPanel) {
    // Poll for results
    {
        let mut finished: Option<anyhow::Result<Vec<DupeGroup>>> = None;
        if let DupesStatus::Loading(ref rx) = p.status {
            if let Ok(r) = rx.try_recv() {
                finished = Some(r);
            }
        }
        if let Some(r) = finished {
            p.selected_group = None;
            p.status = match r {
                Ok(groups) => DupesStatus::Done(groups),
                Err(e) => DupesStatus::Error(e.to_string()),
            };
        }
    }

    ui.add_space(8.0);
    ui.heading("Duplicate Files");
    ui.add_space(6.0);

    let loading = matches!(p.status, DupesStatus::Loading(_));

    ui.horizontal(|ui| {
        ui.label("Min size (bytes):");
        ui.add_enabled(
            !loading,
            TextEdit::singleline(&mut p.min_size_str).desired_width(100.0),
        );
        if ui.add_enabled(!loading, egui::Button::new("Find Duplicates")).clicked() {
            let db_path = db.to_string();
            let min_size: u64 = p.min_size_str.parse().unwrap_or(1);
            let (tx, rx) = mpsc::channel();
            std::thread::spawn(move || {
                let result = (|| -> anyhow::Result<Vec<DupeGroup>> {
                    let conn = db::open(std::path::Path::new(&db_path))?;
                    db::query_dupes(&conn, min_size)
                })();
                tx.send(result).ok();
            });
            p.status = DupesStatus::Loading(rx);
        }
    });

    ui.add_space(8.0);
    ui.separator();
    ui.add_space(4.0);

    match &p.status {
        DupesStatus::Idle => {
            ui.label(RichText::new("Press [Find Duplicates] to search.").color(Color32::GRAY));
            return;
        }
        DupesStatus::Loading(_) => {
            ui.horizontal(|ui| { ui.spinner(); ui.label("Querying database…"); });
            return;
        }
        DupesStatus::Error(e) => {
            ui.label(RichText::new(format!("✖  {}", e)).color(Color32::RED));
            return;
        }
        DupesStatus::Done(groups) if groups.is_empty() => {
            ui.label(RichText::new("✔  No duplicates found.").color(Color32::from_rgb(80, 200, 80)));
            return;
        }
        _ => {}
    }

    // Collect lightweight row data and selected-group paths without holding a
    // borrow on p.status while we also mutate p.selected_group below.
    struct RowMeta { sha256_prefix: String, count: u64, size: u64 }
    let (total, rows, selected_paths): (usize, Vec<RowMeta>, Option<Vec<String>>) =
        if let DupesStatus::Done(ref groups) = p.status {
            let rows = groups.iter().map(|g| RowMeta {
                sha256_prefix: g.sha256[..16].to_string(),
                count: g.count,
                size: g.size,
            }).collect();
            let sel = p.selected_group
                .and_then(|i| groups.get(i))
                .map(|g| g.paths.clone());
            (groups.len(), rows, sel)
        } else {
            return;
        };

    ui.label(format!("{} duplicate group(s) — click a row to see paths", total));
    ui.add_space(4.0);

    // ── Paths side panel ─────────────────────────────────────────────────
    egui::SidePanel::right("dupes_paths")
        .min_width(360.0)
        .resizable(true)
        .show_inside(ui, |ui| {
            ui.add_space(4.0);
            if let Some(ref paths) = selected_paths {
                ui.label(RichText::new(format!("{} paths", paths.len())).strong());
                ui.add_space(4.0);
                ScrollArea::vertical()
                    .id_salt("dupes_paths_scroll")
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        for path in paths {
                            ui.horizontal(|ui| {
                                ui.label(
                                    RichText::new(truncate_path(path, 50))
                                        .monospace()
                                        .color(Color32::LIGHT_GRAY),
                                )
                                .on_hover_text(path.as_str());
                                if ui.small_button("Open in App").clicked() { open_path(path); }
                                if ui.small_button("Open Folder").clicked() { open_parent(path); }
                            });
                        }
                    });
            } else {
                ui.label(RichText::new("Select a group to see paths.").color(Color32::GRAY));
            }
        });

    // ── Virtualized group list ────────────────────────────────────────────
    // Each collapsed row is one fixed-height selectable label. show_rows only
    // renders the visible slice, so 100k groups scroll smoothly.
    let row_height = ui.spacing().interact_size.y + ui.spacing().item_spacing.y;
    ScrollArea::vertical()
        .id_salt("dupes_scroll")
        .auto_shrink([false, false]) // task #8: fill panel width so scrollbar is at the edge
        .show_rows(ui, row_height, total, |ui, row_range| {
            for i in row_range {
                let r = &rows[i];
                let label = format!(
                    "[{}…]  {} copies  {}",
                    r.sha256_prefix, r.count, human_size(r.size)
                );
                let selected = p.selected_group == Some(i);
                if ui.selectable_label(selected, label).clicked() {
                    p.selected_group = if selected { None } else { Some(i) };
                }
            }
        });
}

// ── Find panel ───────────────────────────────────────────────────────────────

fn show_find(ui: &mut egui::Ui, ctx: &egui::Context, db: &str, p: &mut FindPanel) {
    // Poll file dialog
    if let Some(ref rx) = p.file_dialog_rx {
        if let Ok(opt) = rx.try_recv() {
            if let Some(path) = opt {
                p.input = path.to_string_lossy().to_string();
            }
            p.file_dialog_rx = None;
        }
    }

    // Poll for find results
    {
        let mut done_result: Option<anyhow::Result<FindResult>> = None;
        if let FindStatus::Loading(ref rx) = p.status {
            if let Ok(r) = rx.try_recv() {
                done_result = Some(r);
            }
        }
        if let Some(r) = done_result {
            p.status = match r {
                Ok(res) => FindStatus::Done(res),
                Err(e) => FindStatus::Error(e.to_string()),
            };
        }
    }

    // Poll for image preview load
    {
        let mut loaded: Option<egui::ColorImage> = None;
        let mut done = false;
        if let Some(ref rx) = p.preview_rx {
            match rx.try_recv() {
                Ok(img) => { loaded = Some(img); done = true; }
                Err(mpsc::TryRecvError::Disconnected) => { done = true; } // thread exited (decode failed)
                Err(mpsc::TryRecvError::Empty) => {}
            }
        }
        if done { p.preview_rx = None; }
        if let Some(img) = loaded {
            p.preview_texture =
                Some(ctx.load_texture("preview", img, egui::TextureOptions::default()));
        }
    }

    ui.add_space(8.0);
    ui.heading("Find by Hash or File");
    ui.add_space(6.0);

    let loading = matches!(p.status, FindStatus::Loading(_));

    ui.horizontal(|ui| {
        ui.label("Input:");
        ui.add_enabled(
            !loading,
            TextEdit::singleline(&mut p.input)
                .desired_width(420.0)
                .hint_text("SHA-256 hash (64 hex chars) or file path"),
        );
        if ui.add_enabled(!loading, egui::Button::new("Browse…")).clicked() {
            let (tx, rx) = mpsc::channel();
            std::thread::spawn(move || {
                let path = rfd::FileDialog::new()
                    .set_title("Select file to find duplicates of")
                    .pick_file();
                tx.send(path).ok();
            });
            p.file_dialog_rx = Some(rx);
        }
    });

    // Hide PDQ threshold when input already looks like a raw hash (task #5)
    let input_is_hash = p.input.len() == 64 && p.input.bytes().all(|b| b.is_ascii_hexdigit());
    if !input_is_hash {
        ui.horizontal(|ui| {
            ui.label("PDQ threshold:");
            ui.add(Slider::new(&mut p.threshold, 0..=256).text("bits"));
        });
    }

    ui.add_space(6.0);
    let can_search = !loading && !p.input.is_empty();
    if ui.add_enabled(can_search, egui::Button::new("Search")).clicked() {
        let input = p.input.trim().to_string();
        let db_path = db.to_string();
        let threshold = p.threshold;
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            tx.send(run_find(&input, &db_path, threshold)).ok();
        });
        p.status = FindStatus::Loading(rx);
        p.selected_path = None;
        p.preview_texture = None;
        p.preview_rx = None; // task #2: clear any in-flight image load
    }

    ui.add_space(8.0);
    ui.separator();

    // Collect display data from p.status without holding a borrow while we
    // mutably borrow other fields of p later.
    enum DisplayState {
        Idle,
        Searching,
        Err(String),
        Results {
            sha256: Option<String>,
            exact: Vec<String>,
            similar: Vec<(String, u32)>,
        },
    }
    let display = match &p.status {
        FindStatus::Idle => DisplayState::Idle,
        FindStatus::Loading(_) => DisplayState::Searching,
        FindStatus::Error(e) => DisplayState::Err(e.clone()),
        FindStatus::Done(res) => DisplayState::Results {
            sha256: res.sha256.clone(),
            exact: res.exact.clone(),
            similar: res.similar.iter().map(|s| (s.path.clone(), s.distance)).collect(),
        },
    };

    // ── Preview pane (right side panel) ──────────────────────────────────
    // Using SidePanel so the remaining ui keeps a vertical layout for results.
    // task #1: this replaces the horizontal_top + allocate_ui split that was
    // causing all result rows to be placed horizontally.
    egui::SidePanel::right("find_preview")
        .min_width(320.0)
        .max_width(420.0)
        .resizable(true)
        .show_inside(ui, |ui| {
            ui.add_space(4.0);
            ui.label(RichText::new("Preview").strong());
            ui.add_space(4.0);
            if let Some(ref path) = p.selected_path.clone() {
                ui.label(
                    RichText::new(truncate_path(path, 40))
                        .monospace()
                        .color(Color32::GRAY),
                )
                .on_hover_text(path.as_str());
                ui.add_space(4.0);
                ui.horizontal(|ui| {
                    if ui.small_button("Open in App").clicked() {
                        open_path(path);
                    }
                    if ui.small_button("Open Folder").clicked() {
                        open_parent(path);
                    }
                });
                ui.add_space(6.0);

                if p.preview_rx.is_some() {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label("Loading preview…");
                    });
                } else if let Some(ref tex) = p.preview_texture {
                    let avail = ui.available_size();
                    let max_w = avail.x - 8.0;
                    let max_h = (avail.y - 8.0).max(1.0);
                    let tw = tex.size()[0] as f32;
                    let th = tex.size()[1] as f32;
                    let scale = (max_w / tw).min(max_h / th).min(1.0);
                    let size = Vec2::new(tw * scale, th * scale);
                    ui.image((tex.id(), size));
                } else {
                    ui.label(RichText::new("(not an image)").color(Color32::GRAY));
                }
            } else {
                ui.label(RichText::new("Select a result to preview.").color(Color32::GRAY));
            }
        });

    // ── Results list (remaining space) ───────────────────────────────────
    match display {
        DisplayState::Idle => {
            ui.label(
                RichText::new("Enter a hash or pick a file, then press Search.")
                    .color(Color32::GRAY),
            );
        }
        DisplayState::Searching => {
            ui.horizontal(|ui| {
                ui.spinner();
                ui.label("Searching…");
            });
        }
        DisplayState::Err(e) => {
            ui.label(RichText::new(format!("✖  {}", e)).color(Color32::RED));
        }
        DisplayState::Results { sha256, exact, similar } => {
            // SHA-256 label lives above the scroll area (always visible)
            if let Some(sha) = sha256 {
                ui.label(
                    RichText::new(format!("SHA-256: {}", sha))
                        .monospace()
                        .color(Color32::GRAY),
                );
                ui.add_space(4.0);
            }

            // Flatten results + section headers into a uniform row list so we
            // can use show_rows for virtual rendering.
            enum FindRow {
                ExactHeader(usize),
                Exact(String),
                SimilarHeader(usize),
                Similar(String, u32),
                NoMatches,
            }
            let mut rows: Vec<FindRow> = Vec::new();
            if exact.is_empty() && similar.is_empty() {
                rows.push(FindRow::NoMatches);
            } else {
                if !exact.is_empty() {
                    rows.push(FindRow::ExactHeader(exact.len()));
                    for path in exact { rows.push(FindRow::Exact(path)); }
                }
                if !similar.is_empty() {
                    rows.push(FindRow::SimilarHeader(similar.len()));
                    for (path, dist) in similar { rows.push(FindRow::Similar(path, dist)); }
                }
            }

            let row_height = ui.spacing().interact_size.y + ui.spacing().item_spacing.y;
            let total_rows = rows.len();
            ScrollArea::vertical()
                .id_salt("find_scroll")
                .auto_shrink([false, false])
                .show_rows(ui, row_height, total_rows, |ui, row_range| {
                    for i in row_range {
                        match &rows[i] {
                            FindRow::NoMatches => {
                                ui.label(RichText::new("No matches found.").color(Color32::GRAY));
                            }
                            FindRow::ExactHeader(n) => {
                                ui.label(RichText::new(format!("Exact matches ({})", n)).strong());
                            }
                            FindRow::SimilarHeader(n) => {
                                ui.label(RichText::new(format!("Perceptually similar images ({})", n)).strong());
                            }
                            FindRow::Exact(path) => {
                                show_result_row(
                                    ui, path, None,
                                    &mut p.selected_path,
                                    &mut p.preview_rx,
                                    &mut p.preview_texture,
                                );
                            }
                            FindRow::Similar(path, dist) => {
                                show_result_row(
                                    ui, path, Some(*dist),
                                    &mut p.selected_path,
                                    &mut p.preview_rx,
                                    &mut p.preview_texture,
                                );
                            }
                        }
                    }
                });
        }
    }
}

fn show_result_row(
    ui: &mut egui::Ui,
    path: &str,
    distance: Option<u32>,
    selected_path: &mut Option<String>,
    preview_rx: &mut Option<mpsc::Receiver<egui::ColorImage>>,
    preview_texture: &mut Option<egui::TextureHandle>,
) {
    ui.horizontal(|ui| {
        let short = truncate_path(path, 60);
        let label = match distance {
            Some(d) => format!("[{:>3}b] {}", d, short),
            None => short,
        };
        let is_selected = selected_path.as_deref() == Some(path);
        let resp = ui.selectable_label(is_selected, RichText::new(&label).monospace().small());
        // Show full path on hover (task #4)
        resp.clone().on_hover_text(path);
        if resp.clicked() {
            *selected_path = Some(path.to_string());
            *preview_texture = None;
            // Drop old in-flight load before starting a new one (task #6)
            *preview_rx = None;
            // task #3: use MIME detection (reads file header) instead of extension check
            let is_image = tree_magic_mini::from_filepath(std::path::Path::new(path))
                .map(|m| m.starts_with("image/"))
                .unwrap_or(false);
            if is_image {
                let path_owned = path.to_string();
                let (tx, rx) = mpsc::channel();
                std::thread::spawn(move || {
                    if let Ok(img) = image::open(&path_owned) {
                        let img = img.to_rgba8();
                        let (w, h) = img.dimensions();
                        // Downscale to max 1024 on the long side
                        let (dw, dh) = if w > 1024 || h > 1024 {
                            let s = 1024.0 / (w.max(h) as f32);
                            ((w as f32 * s) as u32, (h as f32 * s) as u32)
                        } else {
                            (w, h)
                        };
                        let img = if (dw, dh) != (w, h) {
                            image::imageops::resize(
                                &img,
                                dw,
                                dh,
                                image::imageops::FilterType::Triangle,
                            )
                        } else {
                            img
                        };
                        let (fw, fh) = img.dimensions();
                        let pixels = img.into_raw();
                        let color_img = egui::ColorImage::from_rgba_unmultiplied(
                            [fw as usize, fh as usize],
                            &pixels,
                        );
                        tx.send(color_img).ok();
                    }
                });
                *preview_rx = Some(rx);
            }
        }
        // task #7: consistent button labels
        if ui.small_button("Open in App").clicked() {
            open_path(path);
        }
        if ui.small_button("Open Folder").clicked() {
            open_parent(path);
        }
    });
}

fn run_find(input: &str, db_path: &str, threshold: u32) -> anyhow::Result<FindResult> {
    let is_hash = input.len() == 64 && input.bytes().all(|b| b.is_ascii_hexdigit());

    if is_hash {
        let hash = input.to_lowercase();
        let conn = db::open(std::path::Path::new(db_path))?;
        let exact = db::query_by_hash(&conn, &hash)?;
        return Ok(FindResult { exact, similar: vec![], sha256: Some(hash) });
    }

    let path = std::path::Path::new(input);
    if !path.exists() {
        anyhow::bail!("Path does not exist: {}", input);
    }

    let sha256 = scan::sha256_file(path)?;
    let conn = db::open(std::path::Path::new(db_path))?;
    let exact = db::query_by_hash(&conn, &sha256)?;

    let mime = tree_magic_mini::from_filepath(path).unwrap_or("application/octet-stream");
    let similar = if mime.starts_with("image/") {
        match scan::compute_pdq(path) {
            Some(pdq) => db::query_similar_pdq(&conn, &pdq, threshold)?,
            None => vec![],
        }
    } else {
        vec![]
    };

    Ok(FindResult { exact, similar, sha256: Some(sha256) })
}

// ── Stats panel ──────────────────────────────────────────────────────────────

fn show_stats(ui: &mut egui::Ui, db: &str, p: &mut StatsPanel) {
    let mut done: Option<anyhow::Result<Stats>> = None;
    if let StatsStatus::Loading(ref rx) = p.status {
        if let Ok(r) = rx.try_recv() {
            done = Some(r);
        }
    }
    if let Some(r) = done {
        p.status = match r {
            Ok(s) => StatsStatus::Done(s),
            Err(e) => StatsStatus::Error(e.to_string()),
        };
    }

    ui.add_space(8.0);
    ui.heading("Database Statistics");
    ui.add_space(6.0);

    let loading = matches!(p.status, StatsStatus::Loading(_));
    if ui.add_enabled(!loading, egui::Button::new("Refresh")).clicked() {
        let db_path = db.to_string();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let r = (|| -> anyhow::Result<Stats> {
                let conn = db::open(std::path::Path::new(&db_path))?;
                db::query_stats(&conn)
            })();
            tx.send(r).ok();
        });
        p.status = StatsStatus::Loading(rx);
    }

    ui.add_space(8.0);
    ui.separator();
    ui.add_space(6.0);

    match &p.status {
        StatsStatus::Idle => {
            ui.label(RichText::new("Press [Refresh] to load statistics.").color(Color32::GRAY));
        }
        StatsStatus::Loading(_) => {
            ui.horizontal(|ui| {
                ui.spinner();
                ui.label("Loading…");
            });
        }
        StatsStatus::Error(e) => {
            ui.label(RichText::new(format!("✖  {}", e)).color(Color32::RED));
        }
        StatsStatus::Done(s) => {
            egui::Grid::new("stats_grid")
                .num_columns(2)
                .spacing([40.0, 6.0])
                .show(ui, |ui| {
                    stat_row(ui, "Total files", s.total_files);
                    ui.label("Total size:");
                    ui.label(human_size(s.total_size));
                    ui.end_row();
                    stat_row(ui, "Duplicate groups", s.dupe_groups);
                    stat_row(ui, "Duplicate files", s.dupe_files);
                    stat_row(ui, "Errors", s.error_count);
                    stat_row(ui, "Stale", s.stale_count);
                });
        }
    }
}

// ── Stale panel ──────────────────────────────────────────────────────────────

fn show_stale(ui: &mut egui::Ui, db: &str, p: &mut StalePanel) {
    let mut done: Option<anyhow::Result<Vec<String>>> = None;
    if let StaleStatus::Loading(ref rx) = p.status {
        if let Ok(r) = rx.try_recv() {
            done = Some(r);
        }
    }
    if let Some(r) = done {
        p.status = match r {
            Ok(paths) => StaleStatus::Done(paths),
            Err(e) => StaleStatus::Error(e.to_string()),
        };
    }

    ui.add_space(8.0);
    ui.heading("Stale Files");
    ui.add_space(6.0);

    let loading = matches!(p.status, StaleStatus::Loading(_));
    if ui.add_enabled(!loading, egui::Button::new("Refresh")).clicked() {
        let db_path = db.to_string();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let r = (|| -> anyhow::Result<Vec<String>> {
                let conn = db::open(std::path::Path::new(&db_path))?;
                db::query_stale(&conn)
            })();
            tx.send(r).ok();
        });
        p.status = StaleStatus::Loading(rx);
    }

    ui.add_space(8.0);
    ui.separator();
    ui.add_space(4.0);

    match &p.status {
        StaleStatus::Idle => {
            ui.label(
                RichText::new("Press [Refresh] to list stale files.").color(Color32::GRAY),
            );
        }
        StaleStatus::Loading(_) => {
            ui.horizontal(|ui| {
                ui.spinner();
                ui.label("Loading…");
            });
        }
        StaleStatus::Error(e) => {
            ui.label(RichText::new(format!("✖  {}", e)).color(Color32::RED));
        }
        StaleStatus::Done(paths) => {
            if paths.is_empty() {
                ui.label(
                    RichText::new("✔  No stale files.")
                        .color(Color32::from_rgb(80, 200, 80)),
                );
            } else {
                ui.label(format!("{} stale file(s):", paths.len()));
                ui.add_space(4.0);
                ScrollArea::vertical().id_salt("stale_scroll").show(ui, |ui| {
                    for path in paths {
                        ui.horizontal(|ui| {
                            ui.label(
                                RichText::new(path).monospace().color(Color32::LIGHT_GRAY),
                            );
                            if ui.small_button("Open Folder").clicked() {
                                open_parent(path);
                            }
                        });
                    }
                });
            }
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn stat_row(ui: &mut egui::Ui, label: &str, value: u64) {
    ui.label(format!("{}:", label));
    ui.label(value.to_string());
    ui.end_row();
}

fn human_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} B", bytes)
    } else {
        format!("{:.2} {}", size, UNITS[unit])
    }
}

fn truncate_path(path: &str, max: usize) -> String {
    if path.len() <= max {
        path.to_string()
    } else {
        format!("…{}", &path[path.len().saturating_sub(max - 1)..])
    }
}


fn open_path(path: &str) {
    let _ = open::that(path);
}

fn open_parent(path: &str) {
    if let Some(parent) = std::path::Path::new(path).parent() {
        let _ = open::that(parent);
    }
}
