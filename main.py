#!/usr/bin/env python3
# -*- coding: utf-8 -*-
 
import argparse, csv, json, shutil, subprocess, sys
from pathlib import Path
from typing import Optional, Tuple, List, Dict
 
# ===== Configurações gerais =====
RAW_EXTS = {".h264", ".hevc", ".dav"}  # bitstreams crus comuns em CFTV
VIDEO_EXTS = {
    ".mp4",".mov",".mkv",".avi",".ts",".m2ts",".flv",".webm",".3gp",
    ".mpg",".mpeg",".wmv",".asf",".m4v"
} | RAW_EXTS
 
# ===== Utilidades de execução =====
def run(cmd: List[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8")
 
def ok_tools() -> bool:
    return shutil.which("ffmpeg") is not None and shutil.which("ffprobe") is not None
 
# ===== ffprobe helpers =====
def ffprobe_json(path: Path) -> Optional[dict]:
    cmd = ["ffprobe","-v","error","-print_format","json","-show_format","-show_streams",
           "-analyzeduration","200M","-probesize","200M", str(path)]
    p = run(cmd)
    if p.returncode != 0: return None
    try: return json.loads(p.stdout or "{}")
    except Exception: return None
 
def get_meta(path: Path) -> Tuple[Optional[float], Optional[str], Optional[str], Optional[float]]:
    """
    Retorna: (meta_duration_s, container(format_name), vcodec(codec_name), start_time_s)
    """
    data = ffprobe_json(path)
    if not data: return None, None, None, None
    fmt = data.get("format", {}) or {}
    streams = data.get("streams", []) or []
    dur = None
    try:
        d = fmt.get("duration")
        dur = float(d) if d is not None else None
    except Exception:
        dur = None
    container = fmt.get("format_name") or ""
    start_time = None
    try:
        start_time = float(fmt.get("start_time"))
    except Exception:
        start_time = None
    vcodec = None
    if streams:
        for st in streams:
            if st.get("codec_type") == "video":
                vcodec = st.get("codec_name")
            # fallback de duração por stream
            if dur is None:
                sd = st.get("duration")
                try:
                    if sd is not None:
                        sv = float(sd)
                        if sv > 0: dur = sv if dur is None else max(dur, sv)
                except Exception:
                    pass
    return dur, container, vcodec, start_time
 
def get_packet_first_last_pts(path: Path) -> Tuple[Optional[float], Optional[float]]:
    """
    Retorna (first_pts, last_pts) lendo pts_time dos pacotes de vídeo v:0.
    """
    cmd = ["ffprobe","-v","error","-select_streams","v:0","-show_packets",
           "-show_entries","packet=pts_time","-of","csv=p=0","-read_intervals","%+#999999",
           "-analyzeduration","200M","-probesize","200M", str(path)]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, encoding="utf-8", errors="ignore")
    except FileNotFoundError:
        return None, None
    first, last = None, None
    if proc.stdout:
        for line in proc.stdout:
            line = line.strip()
            if not line: continue
            try:
                val = float(line)
            except Exception:
                continue
            if first is None: first = val
            last = val
    proc.wait()
    return first, last
 
# ===== Veredito =====
def verdict_for_file(path: Path, diverge_tol: float = 0.20) -> Tuple[str, str, Dict]:
    """
    Retorna (veredicto, motivo, extras)
      veredicto: "OK" | "WRAP" | "FIX"
    """
    ext = path.suffix.lower()
    if ext in RAW_EXTS:
        return "WRAP", f"Bitstream cru ({ext}); precisa contêiner + FPS.", {"ext": ext}
 
    meta_dur, container, vcodec, start_time = get_meta(path)
    fpts, lpts = get_packet_first_last_pts(path)
 
    if meta_dur is None or meta_dur <= 0.001:
        if fpts is None or lpts is None or lpts <= fpts:
            return "FIX", "Sem duração meta e sem pts_time coerente.", {
                "container": container, "vcodec": vcodec
            }
        pkt_dur = lpts - fpts
        return "FIX", f"Sem duração meta; pacotes sugerem ~{pkt_dur:.3f}s.", {
            "container": container, "vcodec": vcodec, "pkt_duration": pkt_dur
        }
 
    # há duração meta: validar pacotes
    if fpts is None or lpts is None or lpts <= fpts:
        return "FIX", "Meta tem duração, mas pacotes sem pts_time válido (player costuma travar tempo).", {
            "container": container, "vcodec": vcodec, "meta_duration": meta_dur
        }
 
    pkt_dur = max(0.0, lpts - fpts)
    divergence = abs(pkt_dur - meta_dur) / max(meta_dur, 1e-6)
    bad_combo = ("mpeg" in (container or "")) and (vcodec in {"h264", "hevc"})
 
    if divergence > diverge_tol or bad_combo:
        reason = []
        if divergence > diverge_tol:
            reason.append(f"divergência {divergence*100:.1f}% (meta {meta_dur:.3f}s vs pkt {pkt_dur:.3f}s)")
        if bad_combo:
            reason.append(f"container '{container}' com {vcodec} é propenso a timeline ruim")
        return "FIX", "; ".join(reason), {
            "container": container, "vcodec": vcodec, "meta_duration": meta_dur, "pkt_duration": pkt_dur
        }
 
    return "OK", "Metadados e pts_time coerentes.", {
        "container": container, "vcodec": vcodec, "meta_duration": meta_dur, "pkt_duration": pkt_dur
    }
 
# ===== Ações de reparo =====
def ensure_parent(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)
 
def wrap_raw(src: Path, dst: Path, fps: int) -> bool:
    ensure_parent(dst)
    cmd = ["ffmpeg","-y","-hide_banner","-loglevel","error","-fflags","+genpts",
           "-framerate", str(fps), "-i", str(src), "-c", "copy", str(dst)]
    return run(cmd).returncode == 0 and dst.exists() and dst.stat().st_size > 0
 
def remux_to_mkv(src: Path, dst: Path) -> bool:
    ensure_parent(dst)
    cmd = ["ffmpeg","-y","-hide_banner","-loglevel","error","-fflags","+genpts",
           "-analyzeduration","200M","-probesize","200M","-i",str(src),"-c","copy",str(dst)]
    return run(cmd).returncode == 0 and dst.exists() and dst.stat().st_size > 0
 
def mp4_faststart_timescale(src: Path, dst: Path) -> bool:
    ensure_parent(dst)
    cmd = ["ffmpeg","-y","-hide_banner","-loglevel","error","-fflags","+genpts",
           "-i", str(src), "-c", "copy", "-movflags", "+faststart", "-video_track_timescale", "90000", str(dst)]
    return run(cmd).returncode == 0 and dst.exists() and dst.stat().st_size > 0
 
def reencode_cfr(src: Path, dst: Path, out_mkv: bool, fps_in: Optional[int]) -> bool:
    ensure_parent(dst)
    cmd = ["ffmpeg","-y","-hide_banner","-loglevel","error","-fflags","+genpts"]
    if fps_in:  # útil para RAW ou streams tortos
        cmd += ["-framerate", str(fps_in)]
    cmd += ["-i", str(src),
            "-map", "0:v:0?", "-map", "0:a:0?",
            "-vf", "setpts=PTS-STARTPTS",
            "-fps_mode", "cfr",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "20", "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "128k"]
    if not out_mkv:
        cmd += ["-movflags", "+faststart"]
    cmd += [str(dst)]
    return run(cmd).returncode == 0 and dst.exists() and dst.stat().st_size > 0
 
# ===== Pipeline por arquivo =====
def process_one(path: Path, try_fps: List[int], dry_run: bool, inplace: bool, prefer_mkv: bool) -> Tuple[str, str, str, Optional[str]]:
    """
    Retorna (veredito, acao_aplicada, saida, detalhe_erro)
    """
    verdict, reason, extra = verdict_for_file(path)
    # Preparar nomes de saída
    ext_out = ".mkv" if prefer_mkv else ".mp4"
 
    if verdict == "OK":
        return verdict, "none", "", None
 
    if dry_run:
        return verdict, "planned", "", None
 
    # Executar correções
    if verdict == "WRAP":
        # RAW -> embrulhar sem re-encode, tentando FPSs
        for fps in try_fps:
            dst = (path if inplace else path.with_name(path.stem + f"_wrapped{fps}" + ext_out))
            tmp = dst if not inplace else path.with_name(path.stem + f"_wrapped_tmp{ext_out}")
            ok = wrap_raw(path, tmp, fps)
            if ok:
                if inplace:
                    bak = path.with_suffix(path.suffix + ".bak")
                    try: bak.unlink()
                    except: pass
                    path.rename(bak)
                    tmp.rename(path.with_suffix(ext_out))
                    return verdict, f"wrap({fps}) inplace", str(path.with_suffix(ext_out)), None
                return verdict, f"wrap({fps})", str(tmp), None
        return verdict, "wrap_failed", "", "Falha ao embrulhar RAW com FPS tentados."
 
    # FIX: tentar no-reencode primeiro, depois re-encode CFR
    # 1) Remux para MKV (costuma ser mais tolerante)
    mkv_dst = path if inplace else path.with_name(path.stem + "_fixed.mkv")
    mkv_tmp = mkv_dst if not inplace else path.with_name(path.stem + "_fixed_tmp.mkv")
    if remux_to_mkv(path, mkv_tmp):
        if inplace:
            bak = path.with_suffix(path.suffix + ".bak")
            try: bak.unlink()
            except: pass
            path.rename(bak)
            mkv_tmp.rename(path.with_suffix(".mkv"))
            return verdict, "remux_mkv inplace", str(path.with_suffix(".mkv")), None
        return verdict, "remux_mkv", str(mkv_tmp), None
 
    # 2) Tentar MP4 com faststart & timescale
    mp4_dst = path if inplace else path.with_name(path.stem + "_fixed.mp4")
    mp4_tmp = mp4_dst if not inplace else path.with_name(path.stem + "_fixed_tmp.mp4")
    if mp4_faststart_timescale(path, mp4_tmp):
        if inplace:
            bak = path.with_suffix(path.suffix + ".bak")
            try: bak.unlink()
            except: pass
            path.rename(bak)
            mp4_tmp.rename(path)
            return verdict, "mp4_faststart_timescale inplace", str(path), None
        return verdict, "mp4_faststart_timescale", str(mp4_tmp), None
 
    # 3) Fallback: re-encode CFR (gera PTS/DTS válidos)
    for fps_in in [None, *try_fps]:
        dst = path if inplace else path.with_name(path.stem + "_reenc" + ext_out)
        tmp = dst if not inplace else path.with_name(path.stem + "_reenc_tmp" + ext_out)
        ok = reencode_cfr(path, tmp, out_mkv=prefer_mkv, fps_in=fps_in)
        if ok:
            if inplace:
                bak = path.with_suffix(path.suffix + ".bak")
                try: bak.unlink()
                except: pass
                path.rename(bak)
                tmp.rename(path.with_suffix(ext_out))
                return verdict, f"reencode_cfr({fps_in or 'auto'}) inplace", str(path.with_suffix(ext_out)), None
            return verdict, f"reencode_cfr({fps_in or 'auto'})", str(tmp), None
 
    return verdict, "reencode_failed", "", "Re-encode CFR falhou com todos os FPS."
 
# ===== CLI =====
def main():
    ap = argparse.ArgumentParser(description="Audita e corrige vídeos com timeline ruim (CFTV). Tenta no-reencode antes do re-encode CFR.")
    ap.add_argument("entrada", help="Arquivo ou pasta.")
    ap.add_argument("--csv", help="Salvar relatório CSV.")
    ap.add_argument("--tol", type=float, default=0.20, help="Tolerância de divergência meta vs pacotes (padrão 0.20).")
    ap.add_argument("--fps", type=int, action="append", help="FPS a tentar (útil p/ RAW .h264/.hevc/.dav). Pode repetir. Padrão: 25 e 30.")
    ap.add_argument("--dry-run", action="store_true", help="Só audita e informa o que faria; não modifica nada.")
    ap.add_argument("--inplace", action="store_true", help="Substitui o arquivo original (cria .bak).")
    ap.add_argument("--prefer-mkv", action="store_true", help="Gerar MKV como formato de destino preferido.")
    args = ap.parse_args()
 
    if not ok_tools():
        print("ffmpeg/ffprobe não encontrados no PATH.", file=sys.stderr)
        sys.exit(2)
 
    root = Path(args.entrada)
    if not root.exists():
        print(f"Caminho não encontrado: {root}", file=sys.stderr)
        sys.exit(2)
 
    # ajustar tolerância no veredito (função usa valor local)
    global verdict_for_file
    old_vff = verdict_for_file
    def vff(path: Path, diverge_tol: float = args.tol):
        return old_vff(path, diverge_tol=diverge_tol)
    verdict_for_file = vff  # tipo: ignore
 
    try_fps = args.fps if args.fps else [25, 30]
 
    targets = [root] if root.is_file() else [p for p in root.rglob("*") if p.is_file() and p.suffix.lower() in VIDEO_EXTS]
 
    rows = []
    counts = {"OK":0,"WRAP":0,"FIX":0}
    for p in targets:
        verdict, reason, _ = verdict_for_file(p)
        counts[verdict] = counts.get(verdict, 0) + 1
        print(f"[AUDIT:{verdict}] {p} -> {reason}")
 
        action, outpath, err = "none", "", None
        if verdict in {"WRAP","FIX"} and not args.dry_run:
            verdict2, action, outpath, err = process_one(p, try_fps, dry_run=args.dry_run, inplace=args.inplace, prefer_mkv=args.prefer_mkv)[0:4]
            # (verdict2 igual a verdict, mantemos contagem já feita)
 
            if "failed" in action:
                print(f"  -> [ERRO] {err or action}")
            else:
                print(f"  -> [OK] ação: {action}  saída: {outpath}")
 
        rows.append([str(p), verdict, reason, action, outpath, err or ""])
 
    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["arquivo","veredito","motivo","acao","saida","erro"])
            w.writerows(rows)
        print(f"\nRelatório CSV salvo em: {args.csv}")
 
    print(f"\nResumo: OK={counts['OK']}  WRAP={counts['WRAP']}  FIX={counts['FIX']}")
    sys.exit(0)
 
if __name__ == "__main__":
    main()