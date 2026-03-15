use eframe::egui::{self, Color32, RichText, ScrollArea, Slider, TextEdit, Vec2};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc};

use crate::commands::human_size;
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
    Stale,
}

// ── Scan panel ───────────────────────────────────────────────────────────────

enum ScanStatus {
    Idle,
    Running {
        progress: Arc<ScanProgress>,
        result_rx: mpsc::Receiver<anyhow::Result<ScanResult>>,
    },
    /// Scan completed. Second field is the list of (path, error) for files
    /// that could not be hashed, drawn from the database after the scan.
    Done(ScanResult, Vec<(String, String)>),
    Error(String),
}

struct ScanPanel {
    scan_path: String,
    threads: usize,
    status: ScanStatus,
    dir_dialog_rx: Option<mpsc::Receiver<Option<PathBuf>>>,
    /// Database statistics shown in the lower half of the Scan tab.
    stats_status: StatsStatus,
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
            stats_status: StatsStatus::Idle,
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

#[cfg(feature = "cleanup")]
enum CleanupStatus {
    Idle,
    Running(mpsc::Receiver<anyhow::Result<String>>),
    Done(String),
    Error(String),
}

struct DupesPanel {
    min_size_str: String,
    status: DupesStatus,
    selected_group: Option<usize>,
    /// Path selected within the currently-shown group (for the footer Open buttons).
    selected_path: Option<String>,
    #[cfg(feature = "cleanup")]
    cleanup_status: CleanupStatus,
    #[cfg(feature = "cleanup")]
    cleanup_dialog_rx: Option<mpsc::Receiver<Option<PathBuf>>>,
}

impl DupesPanel {
    fn new() -> Self {
        Self {
            min_size_str: "1".to_string(),
            status: DupesStatus::Idle,
            selected_group: None,
            selected_path: None,
            #[cfg(feature = "cleanup")]
            cleanup_status: CleanupStatus::Idle,
            #[cfg(feature = "cleanup")]
            cleanup_dialog_rx: None,
        }
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
    /// When true, use top_n mode; when false, use threshold mode.
    use_top_n: bool,
    top_n: u32,
    status: FindStatus,
    selected_path: Option<String>,
    /// Preview texture for the selected *result* path.
    preview_rx: Option<mpsc::Receiver<egui::ColorImage>>,
    preview_texture: Option<egui::TextureHandle>,
    /// Preview texture for the *source* input image (shown for comparison).
    input_preview_rx: Option<mpsc::Receiver<egui::ColorImage>>,
    input_texture: Option<egui::TextureHandle>,
    file_dialog_rx: Option<mpsc::Receiver<Option<PathBuf>>>,
}

impl FindPanel {
    fn new() -> Self {
        Self {
            input: String::new(),
            threshold: 31,
            use_top_n: false,
            top_n: 3,
            status: FindStatus::Idle,
            selected_path: None,
            preview_rx: None,
            preview_texture: None,
            input_preview_rx: None,
            input_texture: None,
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

// ── Stale panel ──────────────────────────────────────────────────────────────

enum StaleStatus {
    Idle,
    Loading(mpsc::Receiver<anyhow::Result<Vec<String>>>),
    Done(Vec<String>),
    Error(String),
}

enum PurgeStatus {
    Idle,
    Running(mpsc::Receiver<anyhow::Result<usize>>),
    Done(usize),
    Error(String),
}

struct StalePanel {
    status: StaleStatus,
    purge_status: PurgeStatus,
}

impl StalePanel {
    fn new() -> Self {
        Self { status: StaleStatus::Idle, purge_status: PurgeStatus::Idle }
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
            || matches!(self.scan.stats_status, StatsStatus::Loading(_))
            || matches!(self.dupes.status, DupesStatus::Loading(_))
            || matches!(self.find.status, FindStatus::Loading(_))
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
            Ok(res) => {
                // Fetch error details from the DB synchronously; the query is
                // lightweight and only runs once at scan completion.
                let error_files = if res.errors > 0 {
                    db::open(std::path::Path::new(db))
                        .and_then(|conn| db::query_errors(&conn))
                        .unwrap_or_default()
                } else {
                    vec![]
                };
                // Trigger a stats refresh so the new counts are shown immediately.
                p.stats_status = StatsStatus::Idle;
                ScanStatus::Done(res, error_files)
            }
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
        ScanStatus::Done(res, error_files) => {
            if res.cancelled {
                ui.label(
                    RichText::new("⚠  Scan cancelled — unvisited files marked stale. Re-scan to fix.")
                        .color(Color32::YELLOW)
                        .strong(),
                );
            } else {
                ui.label(
                    RichText::new("✔  Scan complete")
                        .color(Color32::from_rgb(80, 200, 80))
                        .strong(),
                );
            }
            ui.add_space(4.0);
            egui::Grid::new("scan_results").num_columns(2).spacing([20.0, 4.0]).show(ui, |ui| {
                stat_row(ui, "Hashed", res.processed);
                stat_row(ui, "Skipped (unchanged)", res.skipped);
                stat_row(ui, "Errors", res.errors);
                stat_row(ui, "Stale (marked missing)", res.stale);
            });
            if !error_files.is_empty() {
                ui.add_space(8.0);
                ui.label(
                    RichText::new(format!("{} file(s) could not be hashed:", error_files.len()))
                        .strong()
                        .color(Color32::YELLOW),
                );
                ui.add_space(4.0);
                let row_height = ui.text_style_height(&egui::TextStyle::Monospace)
                    + ui.spacing().item_spacing.y * 2.0;
                ScrollArea::vertical()
                    .id_salt("scan_errors")
                    .max_height(200.0)
                    .show_rows(ui, row_height, error_files.len(), |ui, range| {
                        for (path, err) in &error_files[range] {
                            ui.horizontal(|ui| {
                                ui.label(
                                    RichText::new(truncate_path(path, 50))
                                        .monospace()
                                        .color(Color32::LIGHT_GRAY)
                                        .small(),
                                )
                                .on_hover_text(path.as_str());
                                ui.label(
                                    RichText::new(err.as_str())
                                        .color(Color32::from_rgb(220, 80, 80))
                                        .small(),
                                );
                            });
                        }
                    });
            }
        }
        ScanStatus::Error(e) => {
            ui.label(RichText::new(format!("✖  Error: {}", e)).color(Color32::RED));
        }
    }

    // ── Database statistics ───────────────────────────────────────────────
    ui.add_space(12.0);
    ui.separator();
    ui.add_space(6.0);

    // Poll for stats completion.
    {
        let mut stats_done: Option<anyhow::Result<Stats>> = None;
        if let StatsStatus::Loading(ref rx) = p.stats_status {
            if let Ok(r) = rx.try_recv() {
                stats_done = Some(r);
            }
        }
        if let Some(r) = stats_done {
            p.stats_status = match r {
                Ok(s) => StatsStatus::Done(s),
                Err(e) => StatsStatus::Error(e.to_string()),
            };
        }
    }

    // Auto-load on Idle (first render, after scan, after Refresh).
    let stats_loading = matches!(p.stats_status, StatsStatus::Loading(_));
    let refresh_clicked = {
        ui.horizontal(|ui| {
            ui.heading("Database Statistics");
            ui.add_enabled(!stats_loading, egui::Button::new("Refresh"))
                .clicked()
        })
        .inner
    };
    if (matches!(p.stats_status, StatsStatus::Idle) || refresh_clicked) && !stats_loading {
        let db_path = db.to_string();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let r = (|| -> anyhow::Result<Stats> {
                let conn = db::open(std::path::Path::new(&db_path))?;
                db::query_stats(&conn)
            })();
            tx.send(r).ok();
        });
        p.stats_status = StatsStatus::Loading(rx);
    }

    ui.add_space(4.0);
    match &p.stats_status {
        StatsStatus::Idle | StatsStatus::Loading(_) => {
            ui.horizontal(|ui| {
                ui.spinner();
                ui.label("Loading…");
            });
        }
        StatsStatus::Error(e) => {
            ui.label(RichText::new(format!("✖  {}", e)).color(Color32::RED));
        }
        StatsStatus::Done(s) => {
            egui::Grid::new("scan_stats_grid")
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
            p.selected_path = None;
            p.status = match r {
                Ok(groups) => DupesStatus::Done(groups),
                Err(e) => DupesStatus::Error(e.to_string()),
            };
        }
    }

    // Poll cleanup file dialog
    #[cfg(feature = "cleanup")]
    {
        let mut chosen: Option<Option<PathBuf>> = None;
        if let Some(ref rx) = p.cleanup_dialog_rx {
            if let Ok(r) = rx.try_recv() {
                chosen = Some(r);
            }
        }
        if let Some(opt_path) = chosen {
            p.cleanup_dialog_rx = None;
            if let Some(output_path) = opt_path {
                let db_path = db.to_string();
                let min_size: u64 = crate::commands::parse_human_size(&p.min_size_str).unwrap_or(1);
                let (tx, rx) = mpsc::channel();
                std::thread::spawn(move || {
                    let result = (|| -> anyhow::Result<String> {
                        let conn = db::open(std::path::Path::new(&db_path))?;
                        let groups = db::query_dupes_for_cleanup(&conn, min_size)?;
                        if groups.is_empty() {
                            return Ok("No duplicate groups found — nothing written.".to_string());
                        }
                        let file = std::fs::File::create(&output_path)?;
                        let mut writer = std::io::BufWriter::new(file);
                        let n = crate::cleanup::generate_script(
                            &groups,
                            std::path::Path::new(&db_path),
                            &output_path,
                            &mut writer,
                        )?;
                        Ok(format!("Wrote {} group(s) to {}", n, output_path.display()))
                    })();
                    tx.send(result).ok();
                });
                p.cleanup_status = CleanupStatus::Running(rx);
            }
        }
    }

    // Poll cleanup generation result
    #[cfg(feature = "cleanup")]
    {
        let mut done: Option<anyhow::Result<String>> = None;
        if let CleanupStatus::Running(ref rx) = p.cleanup_status {
            if let Ok(r) = rx.try_recv() {
                done = Some(r);
            }
        }
        if let Some(r) = done {
            p.cleanup_status = match r {
                Ok(msg) => CleanupStatus::Done(msg),
                Err(e) => CleanupStatus::Error(e.to_string()),
            };
        }
    }

    ui.add_space(8.0);
    ui.heading("Duplicate Files");
    ui.add_space(6.0);

    let loading = matches!(p.status, DupesStatus::Loading(_));
    #[cfg(feature = "cleanup")]
    let cleanup_busy = matches!(p.cleanup_status, CleanupStatus::Running(_));

    ui.horizontal(|ui| {
        ui.label("Min size (bytes):");
        ui.add_enabled(
            !loading,
            TextEdit::singleline(&mut p.min_size_str)
                .desired_width(120.0)
                .hint_text("e.g. 1, 512KB, 1.5MB"),
        );
        if ui.add_enabled(!loading, egui::Button::new("Find Duplicates")).clicked() {
            let db_path = db.to_string();
            let min_size: u64 = crate::commands::parse_human_size(&p.min_size_str).unwrap_or(1);
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
        #[cfg(feature = "cleanup")]
        {
            ui.separator();
            let gen_btn = ui.add_enabled(
                !cleanup_busy && !loading,
                egui::Button::new("Generate Cleanup Script…"),
            );
            if gen_btn.clicked() && p.cleanup_dialog_rx.is_none() {
                let (tx, rx) = mpsc::channel();
                std::thread::spawn(move || {
                    let result = rfd::FileDialog::new()
                        .set_title("Save cleanup script")
                        .add_filter("Shell script", &["sh"])
                        .save_file();
                    tx.send(result).ok();
                });
                p.cleanup_dialog_rx = Some(rx);
            }
            if cleanup_busy {
                ui.spinner();
            }
        }
    });

    // Cleanup status line
    #[cfg(feature = "cleanup")]
    match &p.cleanup_status {
        CleanupStatus::Idle => {}
        CleanupStatus::Running(_) => {
            ui.label(RichText::new("Generating script…").color(Color32::GRAY));
        }
        CleanupStatus::Done(msg) => {
            ui.label(RichText::new(format!("✔  {}", msg)).color(Color32::from_rgb(80, 200, 80)));
        }
        CleanupStatus::Error(e) => {
            ui.label(RichText::new(format!("✖  {}", e)).color(Color32::RED));
        }
    }

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
    // Clone the selected path before the closure so it can be displayed in
    // the footer without conflicting with the mutable borrow of p inside the
    // scroll area.
    let dupes_footer_path = p.selected_path.clone();
    egui::SidePanel::right("dupes_paths")
        .min_width(360.0)
        .resizable(true)
        .show_inside(ui, |ui| {
            ui.add_space(4.0);
            if let Some(ref paths) = selected_paths {
                ui.label(RichText::new(format!("{} paths", paths.len())).strong());
                ui.add_space(4.0);

                // Footer — declared before the scroll so it claims bottom space first.
                egui::TopBottomPanel::bottom("dupes_path_footer")
                    .show_inside(ui, |ui| {
                        ui.separator();
                        ui.add_space(2.0);
                        ui.horizontal(|ui| {
                            match dupes_footer_path.as_deref() {
                                None => {
                                    ui.label(
                                        RichText::new("Select a file to open it")
                                            .color(Color32::GRAY),
                                    );
                                }
                                Some(sel) => {
                                    ui.label(
                                        RichText::new(truncate_path(sel, 40))
                                            .monospace()
                                            .color(Color32::LIGHT_GRAY),
                                    )
                                    .on_hover_text(sel);
                                    if ui.button("Open").clicked() {
                                        open_path(sel);
                                    }
                                    if ui.button("Open Folder").clicked() {
                                        open_parent(sel);
                                    }
                                }
                            }
                        });
                        ui.add_space(2.0);
                    });

                ScrollArea::vertical()
                    .id_salt("dupes_paths_scroll")
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        for path in paths {
                            let is_selected = p.selected_path.as_deref() == Some(path.as_str());
                            if ui.selectable_label(
                                is_selected,
                                RichText::new(truncate_path(path, 50))
                                    .monospace()
                                    .color(Color32::LIGHT_GRAY),
                            )
                            .on_hover_text(path.as_str())
                            .clicked()
                            {
                                p.selected_path = Some(path.clone());
                            }
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
        .auto_shrink([false, false]) // fill panel width so scrollbar is at the edge
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
                    p.selected_path = None;
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

    // Poll for result image preview load
    {
        let mut loaded: Option<egui::ColorImage> = None;
        let mut done = false;
        if let Some(ref rx) = p.preview_rx {
            match rx.try_recv() {
                Ok(img) => { loaded = Some(img); done = true; }
                Err(mpsc::TryRecvError::Disconnected) => { done = true; }
                Err(mpsc::TryRecvError::Empty) => {}
            }
        }
        if done { p.preview_rx = None; }
        if let Some(img) = loaded {
            p.preview_texture =
                Some(ctx.load_texture("preview", img, egui::TextureOptions::default()));
        }
    }

    // Poll for source image preview load
    {
        let mut loaded: Option<egui::ColorImage> = None;
        let mut done = false;
        if let Some(ref rx) = p.input_preview_rx {
            match rx.try_recv() {
                Ok(img) => { loaded = Some(img); done = true; }
                Err(mpsc::TryRecvError::Disconnected) => { done = true; }
                Err(mpsc::TryRecvError::Empty) => {}
            }
        }
        if done { p.input_preview_rx = None; }
        if let Some(img) = loaded {
            p.input_texture =
                Some(ctx.load_texture("input_preview", img, egui::TextureOptions::default()));
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

    // Hide PDQ controls when input already looks like a raw hash.
    let input_is_hash = p.input.len() == 64 && p.input.bytes().all(|b| b.is_ascii_hexdigit());
    if !input_is_hash {
        ui.horizontal(|ui| {
            ui.label("PDQ search mode:");
            ui.radio_value(&mut p.use_top_n, false, "Within threshold")
                .on_hover_text(
                    "Show all images within the specified Hamming distance.\n\
                     PDQ is a 256-bit perceptual hash; the threshold is the maximum\n\
                     number of bits that may differ between two hashes.\n\
                     \n\
                     0 = identical images only.\n\
                     ≤ 31 = near-duplicates (Facebook's recommended default).\n\
                     Higher values cast a wider net and may include false positives.",
                );
            ui.radio_value(&mut p.use_top_n, true, "Top N closest")
                .on_hover_text(
                    "Return the N closest images by perceptual similarity regardless\n\
                     of how similar they are. Useful for confirming that even the\n\
                     nearest match in the database is genuinely dissimilar from your\n\
                     source image — i.e. it is not in the data store.",
                );
        });
        ui.horizontal(|ui| {
            if p.use_top_n {
                ui.label("N:");
                ui.add(egui::DragValue::new(&mut p.top_n).range(1..=100).suffix(" results"));
            } else {
                ui.label("Threshold:");
                ui.add(Slider::new(&mut p.threshold, 0..=256).text("bits"));
            }
        });
    }

    ui.add_space(6.0);
    let can_search = !loading && !p.input.is_empty();
    if ui.add_enabled(can_search, egui::Button::new("Search")).clicked() {
        let input = p.input.trim().to_string();
        let db_path = db.to_string();
        let threshold = p.threshold;
        let top_n = if p.use_top_n { p.top_n as usize } else { 0 };
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            tx.send(run_find(&input, &db_path, threshold, top_n)).ok();
        });
        p.status = FindStatus::Loading(rx);
        p.selected_path = None;
        p.preview_texture = None;
        p.preview_rx = None;
        p.input_texture = None;
        p.input_preview_rx = None;

        // Kick off source image preview load when input is a file path.
        let input_trimmed = p.input.trim().to_string();
        let is_hash = input_trimmed.len() == 64
            && input_trimmed.bytes().all(|b| b.is_ascii_hexdigit());
        if !is_hash {
            let path = std::path::Path::new(&input_trimmed);
            let is_image = tree_magic_mini::from_filepath(path)
                .map(|m| m.starts_with("image/"))
                .unwrap_or(false)
                || {
                    let ext = path.extension()
                        .and_then(|e| e.to_str())
                        .unwrap_or("")
                        .to_lowercase();
                    matches!(ext.as_str(), "jpg"|"jpeg"|"png"|"gif"|"webp"|"bmp"|"tiff"|"tif")
                };
            if is_image && path.exists() {
                let path_owned = input_trimmed.clone();
                let (itx, irx) = mpsc::channel();
                std::thread::spawn(move || {
                    if let Ok(img) = image::open(&path_owned) {
                        let img = img.to_rgba8();
                        let (w, h) = img.dimensions();
                        let (dw, dh) = if w > 512 || h > 512 {
                            let s = 512.0 / (w.max(h) as f32);
                            ((w as f32 * s) as u32, (h as f32 * s) as u32)
                        } else {
                            (w, h)
                        };
                        let img = if (dw, dh) != (w, h) {
                            image::imageops::resize(&img, dw, dh, image::imageops::FilterType::Triangle)
                        } else { img };
                        let (fw, fh) = img.dimensions();
                        let pixels = img.into_raw();
                        itx.send(egui::ColorImage::from_rgba_unmultiplied(
                            [fw as usize, fh as usize], &pixels,
                        )).ok();
                    }
                });
                p.input_preview_rx = Some(irx);
            }
        }
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
    // Collect what we need before the closure borrows p immutably.
    let has_input_tex = p.input_texture.is_some();
    let has_input_loading = p.input_preview_rx.is_some();
    let selected_path_clone = p.selected_path.clone();
    let has_result_loading = p.preview_rx.is_some();

    egui::SidePanel::right("find_preview")
        .min_width(320.0)
        .max_width(420.0)
        .resizable(true)
        .show_inside(ui, |ui| {
            // When we have a source image, split the panel: source on top, match below.
            // Each half gets at most half the available height.
            let show_source = has_input_tex || has_input_loading;
            let show_match = selected_path_clone.is_some() || (!show_source);

            if show_source {
                let half_h = (ui.available_height() / 2.0 - 24.0).max(60.0);
                ui.label(RichText::new("Source").strong());
                ui.add_space(2.0);
                if has_input_loading {
                    ui.horizontal(|ui| { ui.spinner(); ui.label("Loading…"); });
                } else if let Some(ref tex) = p.input_texture {
                    let max_w = ui.available_width() - 8.0;
                    let tw = tex.size()[0] as f32;
                    let th = tex.size()[1] as f32;
                    let scale = (max_w / tw).min(half_h / th).min(1.0);
                    ui.image((tex.id(), Vec2::new(tw * scale, th * scale)));
                }
                ui.add_space(4.0);
                ui.separator();
                ui.add_space(4.0);
            }

            if show_match {
                // Extract the distance for this selected path from display data.
                let match_dist: Option<u32> = if let FindStatus::Done(ref res) = p.status {
                    selected_path_clone.as_deref().and_then(|sel| {
                        res.similar.iter().find(|s| s.path == sel).map(|s| s.distance)
                    })
                } else { None };

                let match_label = match match_dist {
                    Some(d) => format!(
                        "Match  {:.1}% similar  ({} bits)",
                        db::pdq_similarity_pct(d), d
                    ),
                    None => "Match".to_string(),
                };
                ui.label(RichText::new(match_label).strong());
                ui.add_space(2.0);

                if let Some(ref path) = selected_path_clone {
                    ui.label(
                        RichText::new(truncate_path(path, 40))
                            .monospace()
                            .color(Color32::GRAY),
                    )
                    .on_hover_text(path.as_str());
                    ui.add_space(4.0);

                    if has_result_loading {
                        ui.horizontal(|ui| { ui.spinner(); ui.label("Loading preview…"); });
                    } else if let Some(ref tex) = p.preview_texture {
                        let avail = ui.available_size();
                        let max_w = avail.x - 8.0;
                        let max_h = (avail.y - 8.0).max(1.0);
                        let tw = tex.size()[0] as f32;
                        let th = tex.size()[1] as f32;
                        let scale = (max_w / tw).min(max_h / th).min(1.0);
                        ui.image((tex.id(), Vec2::new(tw * scale, th * scale)));
                    } else {
                        ui.label(RichText::new("(not an image)").color(Color32::GRAY));
                    }
                } else {
                    ui.label(RichText::new("Select a result to preview.").color(Color32::GRAY));
                }
            }
        });

    // ── Open footer ──────────────────────────────────────────────────────
    // Clone selected path before closures below take &mut p.
    let find_footer_path = p.selected_path.clone();
    egui::TopBottomPanel::bottom("find_footer")
        .show_inside(ui, |ui| {
            ui.separator();
            ui.add_space(2.0);
            ui.horizontal(|ui| {
                match find_footer_path.as_deref() {
                    None => {
                        ui.label(
                            RichText::new("Select a result to open it").color(Color32::GRAY),
                        );
                    }
                    Some(sel) => {
                        ui.label(
                            RichText::new(truncate_path(sel, 60))
                                .monospace()
                                .color(Color32::LIGHT_GRAY),
                        )
                        .on_hover_text(sel);
                        if ui.button("Open").clicked() {
                            open_path(sel);
                        }
                        if ui.button("Open Folder").clicked() {
                            open_parent(sel);
                        }
                    }
                }
            });
            ui.add_space(2.0);
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
            Some(d) => format!("[{:>5.1}%] {}", db::pdq_similarity_pct(d), short),
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
            // Use MIME detection (reads file header) instead of extension check.
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
    });
}

fn run_find(input: &str, db_path: &str, threshold: u32, top_n: usize) -> anyhow::Result<FindResult> {
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

    // Use MIME detection first; fall back to extension for non-standard files.
    let mime = tree_magic_mini::from_filepath(path).unwrap_or("application/octet-stream");
    let is_image = mime.starts_with("image/") || {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();
        matches!(ext.as_str(), "jpg"|"jpeg"|"png"|"gif"|"webp"|"bmp"|"tiff"|"tif")
    };
    let similar = if is_image {
        let limit = if top_n > 0 { Some(top_n) } else { None };
        match scan::compute_pdq(path) {
            Some(pdq) => db::query_similar_pdq(&conn, &pdq, threshold, limit)?,
            None => vec![],
        }
    } else {
        vec![]
    };

    Ok(FindResult { exact, similar, sha256: Some(sha256) })
}

// ── Stale panel ──────────────────────────────────────────────────────────────

fn show_stale(ui: &mut egui::Ui, db: &str, p: &mut StalePanel) {
    // Poll stale list load
    {
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
    }

    // Poll purge
    {
        let mut done: Option<anyhow::Result<usize>> = None;
        if let PurgeStatus::Running(ref rx) = p.purge_status {
            if let Ok(r) = rx.try_recv() {
                done = Some(r);
            }
        }
        if let Some(r) = done {
            p.purge_status = match r {
                Ok(n) => {
                    // Refresh the list to reflect the now-empty stale set.
                    p.status = StaleStatus::Done(vec![]);
                    PurgeStatus::Done(n)
                }
                Err(e) => PurgeStatus::Error(e.to_string()),
            };
        }
    }

    ui.add_space(8.0);
    ui.heading("Stale Files");
    ui.add_space(6.0);

    let loading = matches!(p.status, StaleStatus::Loading(_));
    let purging = matches!(p.purge_status, PurgeStatus::Running(_));
    let has_stale = matches!(&p.status, StaleStatus::Done(paths) if !paths.is_empty());

    ui.horizontal(|ui| {
        if ui.add_enabled(!loading && !purging, egui::Button::new("Refresh")).clicked() {
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
            p.purge_status = PurgeStatus::Idle;
        }

        let purge_btn = ui.add_enabled(
            has_stale && !purging,
            egui::Button::new("Purge Stale Records"),
        );
        if purge_btn.on_hover_text(
            "Permanently delete all stale records from the database.\n\
             This does not touch files on disk — only removes their\n\
             metadata entries from the HashGoblin database.",
        ).clicked() {
            let db_path = db.to_string();
            let (tx, rx) = mpsc::channel();
            std::thread::spawn(move || {
                let r = (|| -> anyhow::Result<usize> {
                    let conn = db::open(std::path::Path::new(&db_path))?;
                    db::purge_stale(&conn)
                })();
                tx.send(r).ok();
            });
            p.purge_status = PurgeStatus::Running(rx);
        }

        if purging { ui.spinner(); }
    });

    match &p.purge_status {
        PurgeStatus::Idle => {}
        PurgeStatus::Running(_) => {
            ui.label(RichText::new("Purging…").color(Color32::GRAY));
        }
        PurgeStatus::Done(n) => {
            ui.label(
                RichText::new(format!("✔  Purged {} record(s).", n))
                    .color(Color32::from_rgb(80, 200, 80)),
            );
        }
        PurgeStatus::Error(e) => {
            ui.label(RichText::new(format!("✖  {}", e)).color(Color32::RED));
        }
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
                let row_height = ui.spacing().interact_size.y + ui.spacing().item_spacing.y;
                let total = paths.len();
                ScrollArea::vertical()
                    .id_salt("stale_scroll")
                    .auto_shrink([false, false])
                    .show_rows(ui, row_height, total, |ui, range| {
                        for path in &paths[range] {
                            ui.horizontal(|ui| {
                                ui.label(
                                    RichText::new(truncate_path(path, 80))
                                        .monospace()
                                        .color(Color32::LIGHT_GRAY),
                                )
                                .on_hover_text(path.as_str());
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

fn truncate_path(path: &str, max: usize) -> String {
    if path.len() <= max {
        path.to_string()
    } else {
        let boundary = path.floor_char_boundary(path.len().saturating_sub(max - 1));
        format!("…{}", &path[boundary..])
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
