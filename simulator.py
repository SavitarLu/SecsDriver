"""
SECS/HSMS Simulator 桌面 GUI。

运行:
    python3 simulator.py
"""

from __future__ import annotations

import asyncio
import queue
import threading
import tkinter as tk
from concurrent.futures import Future
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText

from src.model_templates import (
    DEFAULT_MODEL_XML,
    MessageTemplate,
    load_model_templates,
)
from src.simulator_core import SimulatorEndpoint, SimulatorEvent


HELP_TEXT = (
    "SxFy 示例: S1F1\n"
    "消息体示例:\n"
    "<L [3]\n"
    '  <A "PING">\n'
    "  <U4 100>\n"
    "  <L [1]\n"
    "    <I2 -1>\n"
    "  >\n"
    ">"
)

MODE_OPTIONS = {
    "Passive (Server)": "passive",
    "Host (Active)": "host",
}
MODE_LABELS = {value: key for key, value in MODE_OPTIONS.items()}


class AsyncLoopThread:
    """为 Tk GUI 托管后台 asyncio 事件循环。"""

    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ready = threading.Event()

    def start(self) -> None:
        if self._thread is not None:
            return

        def runner() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
            self._ready.set()
            loop.run_forever()
            pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()

        self._thread = threading.Thread(target=runner, name="simulator-loop", daemon=True)
        self._thread.start()
        self._ready.wait(timeout=2.0)

    def submit(self, coro) -> Future:
        if self._loop is None:
            raise RuntimeError("后台事件循环尚未启动")
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def stop(self) -> None:
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        self._loop = None
        self._thread = None
        self._ready.clear()


class EndpointPane(ttk.LabelFrame):
    """单窗口单端点的 GUI 面板。"""

    def __init__(self, master, *, app: "SimulatorApp") -> None:
        super().__init__(master, text="Endpoint", padding=12)
        self.app = app

        self.mode_var = tk.StringVar(value="Passive (Server)")
        self.template_path_var = tk.StringVar(value=DEFAULT_MODEL_XML)
        self.template_var = tk.StringVar(value="")
        self.template_desc_var = tk.StringVar(value="未加载模板")
        self.status_var = tk.StringVar(value="STOPPED")
        self.host_var = tk.StringVar(value="127.0.0.1")
        self.port_var = tk.StringVar(value="5000")
        self.device_id_var = tk.StringVar(value="0")
        self.sf_var = tk.StringVar(value="S1F1")
        self.timeout_var = tk.StringVar(value="10")
        self.wait_reply_var = tk.BooleanVar(value=False)
        self.auto_reply_var = tk.BooleanVar(value=True)
        self.mode_hint_var = tk.StringVar(value="被动模式会监听本地端口，等待外部设备或主机连接。")

        self.state_store = {
            "passive": self._default_state(),
            "host": self._default_state(),
        }
        self.state_store["host"]["status"] = "STOPPED"
        self.current_endpoint_key = "passive"
        self.templates: list[MessageTemplate] = []
        self.template_map: dict[str, MessageTemplate] = {}

        self._build()
        self._load_state("passive")
        self.reload_templates(show_message=False)

    def _default_state(self) -> dict:
        return {
            "status": "STOPPED",
            "host": "127.0.0.1",
            "port": "5000",
            "device_id": "0",
            "sf": "S1F1",
            "timeout": "10",
            "wait_reply": False,
            "auto_reply": True,
            "payload": "",
            "reply_payload": "",
            "log": HELP_TEXT + "\n\n",
        }

    def _build(self) -> None:
        header = ttk.Frame(self)
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(5, weight=1)

        ttk.Label(header, text="模式").grid(row=0, column=0, sticky="w")
        mode_combo = ttk.Combobox(
            header,
            textvariable=self.mode_var,
            values=list(MODE_OPTIONS.keys()),
            state="readonly",
            width=18,
        )
        mode_combo.grid(row=0, column=1, sticky="w", padx=(6, 16))
        mode_combo.bind("<<ComboboxSelected>>", self.on_mode_changed)

        ttk.Label(header, text="状态").grid(row=0, column=2, sticky="w")
        ttk.Label(header, textvariable=self.status_var).grid(row=0, column=3, sticky="w", padx=(6, 16))
        ttk.Label(header, textvariable=self.mode_hint_var).grid(row=0, column=5, sticky="w")

        template_source = ttk.Frame(self)
        template_source.grid(row=1, column=0, sticky="ew", pady=(12, 0))
        template_source.columnconfigure(1, weight=1)

        ttk.Label(template_source, text="模板XML").grid(row=0, column=0, sticky="w")
        ttk.Entry(template_source, textvariable=self.template_path_var).grid(
            row=0, column=1, sticky="ew", padx=(6, 8)
        )
        ttk.Button(template_source, text="选择文件", command=self.pick_template_file).grid(
            row=0, column=2, sticky="e"
        )
        ttk.Button(template_source, text="重新加载", command=self.reload_templates).grid(
            row=0, column=3, sticky="e", padx=(8, 0)
        )

        config_frame = ttk.Frame(self)
        config_frame.grid(row=2, column=0, sticky="ew", pady=(12, 0))
        for column in range(6):
            config_frame.columnconfigure(column, weight=1 if column % 2 == 1 else 0)

        ttk.Label(config_frame, text="Host").grid(row=0, column=0, sticky="w")
        ttk.Entry(config_frame, textvariable=self.host_var, width=18).grid(row=0, column=1, sticky="ew", padx=(6, 12))
        ttk.Label(config_frame, text="Port").grid(row=0, column=2, sticky="w")
        ttk.Entry(config_frame, textvariable=self.port_var, width=10).grid(row=0, column=3, sticky="ew", padx=(6, 12))
        ttk.Label(config_frame, text="Device ID").grid(row=0, column=4, sticky="w")
        ttk.Entry(config_frame, textvariable=self.device_id_var, width=10).grid(row=0, column=5, sticky="ew", padx=(6, 0))

        button_frame = ttk.Frame(self)
        button_frame.grid(row=3, column=0, sticky="ew", pady=(12, 0))

        ttk.Button(button_frame, text="启动当前", command=self.start_endpoint).pack(side="left")
        ttk.Button(button_frame, text="停止当前", command=self.stop_endpoint).pack(side="left", padx=(8, 0))
        ttk.Button(button_frame, text="停止全部", command=self.app.stop_all).pack(side="left", padx=(8, 0))
        ttk.Button(button_frame, text="应用自动回复", command=self.apply_auto_reply).pack(side="left", padx=(8, 0))
        ttk.Button(button_frame, text="清空日志", command=self.clear_log).pack(side="right")

        template_frame = ttk.LabelFrame(self, text="消息模板", padding=10)
        template_frame.grid(row=4, column=0, sticky="ew", pady=(12, 0))
        template_frame.columnconfigure(1, weight=1)

        ttk.Label(template_frame, text="模板").grid(row=0, column=0, sticky="w")
        self.template_combo = ttk.Combobox(
            template_frame,
            textvariable=self.template_var,
            state="readonly",
            values=[],
        )
        self.template_combo.grid(row=0, column=1, sticky="ew", padx=(6, 8))
        self.template_combo.bind("<<ComboboxSelected>>", self.on_template_selected)
        ttk.Button(template_frame, text="Primary -> 发送", command=self.load_primary_to_payload).grid(
            row=0, column=2, padx=(0, 8)
        )
        ttk.Button(template_frame, text="Secondary -> 发送", command=self.load_secondary_to_payload).grid(
            row=0, column=3, padx=(0, 8)
        )
        ttk.Button(template_frame, text="Secondary -> 自动回复", command=self.load_secondary_to_reply).grid(
            row=0, column=4
        )
        ttk.Label(template_frame, textvariable=self.template_desc_var).grid(
            row=1, column=0, columnspan=5, sticky="w", pady=(8, 0)
        )

        auto_reply_frame = ttk.Frame(self)
        auto_reply_frame.grid(row=5, column=0, sticky="nsew", pady=(12, 0))
        auto_reply_frame.columnconfigure(0, weight=1)

        ttk.Checkbutton(
            auto_reply_frame,
            text="收到 W-bit 时自动回复 F+1",
            variable=self.auto_reply_var,
            command=self.apply_auto_reply,
        ).grid(row=0, column=0, sticky="w")

        ttk.Label(auto_reply_frame, text="自动回复消息体(SECS结构，可留空)").grid(row=1, column=0, sticky="w", pady=(8, 4))
        self.reply_payload_text = ScrolledText(auto_reply_frame, height=6, wrap="word")
        self.reply_payload_text.grid(row=2, column=0, sticky="nsew")

        send_frame = ttk.Frame(self)
        send_frame.grid(row=6, column=0, sticky="nsew", pady=(12, 0))
        send_frame.columnconfigure(1, weight=1)
        send_frame.columnconfigure(3, weight=1)

        ttk.Label(send_frame, text="SxFy").grid(row=0, column=0, sticky="w")
        ttk.Entry(send_frame, textvariable=self.sf_var, width=12).grid(row=0, column=1, sticky="ew", padx=(6, 12))
        ttk.Label(send_frame, text="Timeout").grid(row=0, column=2, sticky="w")
        ttk.Entry(send_frame, textvariable=self.timeout_var, width=10).grid(row=0, column=3, sticky="ew", padx=(6, 12))
        ttk.Checkbutton(send_frame, text="等待回复", variable=self.wait_reply_var).grid(row=0, column=4, sticky="w")
        ttk.Button(send_frame, text="发送", command=self.send_message).grid(row=0, column=5, sticky="e", padx=(12, 0))

        ttk.Label(send_frame, text="消息体(SECS结构，可留空)").grid(row=1, column=0, columnspan=6, sticky="w", pady=(8, 4))
        self.payload_text = ScrolledText(send_frame, height=8, wrap="word")
        self.payload_text.grid(row=2, column=0, columnspan=6, sticky="nsew")

        log_frame = ttk.Frame(self)
        log_frame.grid(row=7, column=0, sticky="nsew", pady=(12, 0))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(1, weight=1)

        log_header = ttk.Frame(log_frame)
        log_header.grid(row=0, column=0, sticky="ew")
        ttk.Label(log_header, text="收发日志").pack(side="left")
        ttk.Button(log_header, text="复制选中", command=self.copy_log_selection).pack(side="right")
        ttk.Button(log_header, text="复制全部", command=self.copy_log_all).pack(side="right", padx=(0, 8))

        self.log_text = ScrolledText(log_frame, height=24, wrap="word", undo=False)
        self.log_text.grid(row=1, column=0, sticky="nsew")
        self.log_text.configure(state="disabled", cursor="xterm")
        self._bind_log_menu()

        self.columnconfigure(0, weight=1)
        self.rowconfigure(7, weight=1)

    def _bind_log_menu(self) -> None:
        self.log_menu = tk.Menu(self, tearoff=False)
        self.log_menu.add_command(label="复制选中", command=self.copy_log_selection)
        self.log_menu.add_command(label="复制全部", command=self.copy_log_all)
        self.log_text.bind("<Button-3>", self._show_log_menu)
        self.log_text.bind("<Control-Button-1>", self._show_log_menu)

    def _show_log_menu(self, event) -> str:
        self.log_menu.tk_popup(event.x_root, event.y_root)
        return "break"

    def pick_template_file(self) -> None:
        selected = filedialog.askopenfilename(
            title="选择 Model.xml",
            filetypes=[("XML files", "*.xml"), ("All files", "*.*")],
            initialfile="Model.xml",
        )
        if not selected:
            return
        self.template_path_var.set(selected)
        self.reload_templates()

    def reload_templates(self, show_message: bool = True) -> None:
        try:
            templates = load_model_templates(self.template_path_var.get().strip())
        except Exception as exc:
            self.templates = []
            self.template_map = {}
            self.template_combo["values"] = []
            self.template_var.set("")
            self.template_desc_var.set(f"模板加载失败: {exc}")
            if show_message:
                messagebox.showerror("模板加载失败", str(exc))
            return

        self.templates = templates
        labels = [template.label for template in templates]
        self.template_map = {template.label: template for template in templates}
        self.template_combo["values"] = labels

        if labels:
            self.template_var.set(labels[0])
            self._update_template_desc(self.template_map[labels[0]])
        else:
            self.template_var.set("")
            self.template_desc_var.set("未找到模板")

        self.app.sync_template_replies(self._build_template_reply_map())

        if show_message:
            messagebox.showinfo("模板加载完成", f"已加载 {len(templates)} 条 SxFx 模板")

    def on_template_selected(self, _event=None) -> None:
        template = self.get_selected_template()
        if template is not None:
            self._update_template_desc(template)

    def get_selected_template(self) -> MessageTemplate | None:
        label = self.template_var.get().strip()
        if not label:
            messagebox.showinfo("模板", "请先选择一条模板。")
            return None
        template = self.template_map.get(label)
        if template is None:
            messagebox.showinfo("模板", "当前模板无效，请重新选择。")
            return None
        return template

    def _update_template_desc(self, template: MessageTemplate) -> None:
        reply_text = "需要回复" if template.reply_expected else "不需要回复"
        desc = template.description or "无描述"
        self.template_desc_var.set(f"{template.sf} | {desc} | {reply_text}")

    def load_primary_to_payload(self) -> None:
        template = self.get_selected_template()
        if template is None:
            return
        self.sf_var.set(template.sf)
        self.wait_reply_var.set(template.reply_expected)
        self._replace_text(self.payload_text, template.primary_text)

    def load_secondary_to_payload(self) -> None:
        template = self.get_selected_template()
        if template is None:
            return
        self.sf_var.set(template.secondary_sf)
        self.wait_reply_var.set(False)
        self._replace_text(self.payload_text, template.secondary_text)

    def load_secondary_to_reply(self) -> None:
        template = self.get_selected_template()
        if template is None:
            return
        self._replace_text(self.reply_payload_text, template.secondary_text)

    def _build_template_reply_map(self) -> dict[str, str]:
        return {
            template.sf.upper(): template.secondary_text
            for template in self.templates
            if template.secondary_text.strip()
        }

    def on_mode_changed(self, _event=None) -> None:
        previous_key = self.current_endpoint_key
        self._save_state(previous_key)

        self.current_endpoint_key = MODE_OPTIONS[self.mode_var.get()]
        self._load_state(self.current_endpoint_key)

    def _save_state(self, endpoint_key: str) -> None:
        state = self.state_store[endpoint_key]
        state["status"] = self.status_var.get()
        state["host"] = self.host_var.get()
        state["port"] = self.port_var.get()
        state["device_id"] = self.device_id_var.get()
        state["sf"] = self.sf_var.get()
        state["timeout"] = self.timeout_var.get()
        state["wait_reply"] = self.wait_reply_var.get()
        state["auto_reply"] = self.auto_reply_var.get()
        state["payload"] = self.payload_text.get("1.0", "end-1c")
        state["reply_payload"] = self.reply_payload_text.get("1.0", "end-1c")
        state["log"] = self.log_text.get("1.0", "end-1c")

    def _load_state(self, endpoint_key: str) -> None:
        state = self.state_store[endpoint_key]
        self.mode_var.set(MODE_LABELS[endpoint_key])
        self.status_var.set(state["status"])
        self.host_var.set(state["host"])
        self.port_var.set(state["port"])
        self.device_id_var.set(state["device_id"])
        self.sf_var.set(state["sf"])
        self.timeout_var.set(state["timeout"])
        self.wait_reply_var.set(state["wait_reply"])
        self.auto_reply_var.set(state["auto_reply"])
        self.mode_hint_var.set(
            "被动模式会监听本地端口，等待外部设备或主机连接。"
            if endpoint_key == "passive"
            else "主动模式会连接远端 SECS/HSMS 服务。"
        )

        self._replace_text(self.payload_text, state["payload"])
        self._replace_text(self.reply_payload_text, state["reply_payload"])
        self._replace_log(state["log"])

    def _replace_text(self, widget: ScrolledText, content: str) -> None:
        widget.delete("1.0", "end")
        widget.insert("1.0", content)

    def _replace_log(self, content: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.insert("1.0", content)
        self.log_text.configure(state="disabled")
        self.log_text.see("end")

    def start_endpoint(self) -> None:
        endpoint_key = self.current_endpoint_key
        try:
            host = self.host_var.get().strip() or "127.0.0.1"
            port = int(self.port_var.get().strip())
            device_id = int(self.device_id_var.get().strip() or "0")
        except ValueError:
            messagebox.showerror("配置错误", "host/port/device id 不合法")
            return

        self.app.submit(
            endpoint_key,
            self.app.endpoints[endpoint_key].start(
                host,
                port,
                device_id=device_id,
                auto_reply=self.auto_reply_var.get(),
                auto_reply_payload=self.reply_payload_text.get("1.0", "end-1c").strip(),
                auto_reply_templates=self._build_template_reply_map(),
            ),
            action=f"启动 {MODE_LABELS[endpoint_key]}",
        )

    def stop_endpoint(self) -> None:
        endpoint_key = self.current_endpoint_key
        self.app.submit(
            endpoint_key,
            self.app.endpoints[endpoint_key].stop(),
            action=f"停止 {MODE_LABELS[endpoint_key]}",
        )

    def apply_auto_reply(self) -> None:
        endpoint_key = self.current_endpoint_key
        self.app.submit(
            endpoint_key,
            self.app.endpoints[endpoint_key].update_auto_reply(
                self.auto_reply_var.get(),
                self.reply_payload_text.get("1.0", "end-1c").strip(),
                self._build_template_reply_map(),
            ),
            action=f"更新 {MODE_LABELS[endpoint_key]} 自动回复",
            show_errors=False,
        )

    def send_message(self) -> None:
        endpoint_key = self.current_endpoint_key
        try:
            timeout = float(self.timeout_var.get().strip() or "10")
        except ValueError:
            messagebox.showerror("配置错误", "Timeout 必须是数字")
            return

        self.app.submit(
            endpoint_key,
            self.app.endpoints[endpoint_key].send(
                self.sf_var.get(),
                self.payload_text.get("1.0", "end-1c").strip(),
                wait_reply=self.wait_reply_var.get(),
                timeout=timeout,
            ),
            action=f"{MODE_LABELS[endpoint_key]} 发送消息",
        )

    def clear_log(self) -> None:
        state = self.state_store[self.current_endpoint_key]
        state["log"] = HELP_TEXT + "\n\n"
        self._replace_log(state["log"])

    def handle_event(self, event: SimulatorEvent) -> None:
        endpoint_key = event.endpoint
        state = self.state_store[endpoint_key]

        if event.status:
            state["status"] = event.status
            if endpoint_key == self.current_endpoint_key:
                self.status_var.set(event.status)

        if event.kind == "status":
            return

        prefix_map = {
            "log": "INFO",
            "tx": "TX",
            "rx": "RX",
            "reply": "REPLY",
            "error": "ERROR",
        }
        prefix = prefix_map.get(event.kind, event.kind.upper())
        content = f"[{event.timestamp}] [{prefix}]\n{event.text}\n\n"
        state["log"] += content

        if endpoint_key == self.current_endpoint_key:
            self.append_log(content)

    def append_log(self, content: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.insert("end", content)
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def copy_log_selection(self) -> None:
        try:
            selected = self.log_text.get("sel.first", "sel.last")
        except tk.TclError:
            messagebox.showinfo("复制日志", "请先在日志区选中要复制的内容。")
            return
        self.clipboard_clear()
        self.clipboard_append(selected)

    def copy_log_all(self) -> None:
        content = self.log_text.get("1.0", "end-1c")
        self.clipboard_clear()
        self.clipboard_append(content)


class SimulatorApp(tk.Tk):
    """主窗口。"""

    def __init__(self) -> None:
        super().__init__()
        self.title("SECS / HSMS Simulator")
        self.geometry("1080x920")
        self.minsize(920, 760)

        self.loop_thread = AsyncLoopThread()
        self.loop_thread.start()

        self.event_queue: "queue.Queue[SimulatorEvent]" = queue.Queue()
        self.endpoints = {
            "passive": SimulatorEndpoint("passive", "passive", self.event_queue.put),
            "host": SimulatorEndpoint("host", "active", self.event_queue.put),
        }

        self._build()
        self.after(100, self._drain_events)
        self.protocol("WM_DELETE_WINDOW", self.on_close)

    def _build(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        toolbar = ttk.Frame(self, padding=(12, 12, 12, 0))
        toolbar.grid(row=0, column=0, sticky="ew")
        ttk.Label(
            toolbar,
            text="单窗口选择 Passive 或 Host，适合分别起多个实例做联调。",
        ).pack(side="left")

        content = ttk.Frame(self, padding=12)
        content.grid(row=1, column=0, sticky="nsew")
        content.columnconfigure(0, weight=1)
        content.rowconfigure(0, weight=1)

        self.endpoint_pane = EndpointPane(content, app=self)
        self.endpoint_pane.grid(row=0, column=0, sticky="nsew")

    def submit(self, endpoint_name: str, coro, *, action: str, show_errors: bool = True) -> None:
        future = self.loop_thread.submit(coro)

        def on_done(done: Future) -> None:
            try:
                done.result()
            except Exception as exc:
                if show_errors:
                    self.after(0, lambda: messagebox.showerror(action, str(exc)))
                self.event_queue.put(
                    SimulatorEvent(
                        endpoint=endpoint_name,
                        kind="error",
                        text=f"{action}失败: {exc}",
                    )
                )

        future.add_done_callback(on_done)

    def sync_template_replies(self, template_replies: dict[str, str]) -> None:
        for endpoint_key, endpoint in self.endpoints.items():
            self.submit(
                endpoint_key,
                endpoint.update_auto_reply_templates(template_replies),
                action=f"同步 {MODE_LABELS[endpoint_key]} 模板回复",
                show_errors=False,
            )

    def stop_all(self) -> None:
        for endpoint_key, endpoint in self.endpoints.items():
            self.submit(
                endpoint_key,
                endpoint.stop(),
                action=f"停止 {MODE_LABELS[endpoint_key]}",
                show_errors=False,
            )

    def _drain_events(self) -> None:
        if not self.winfo_exists():
            return
        while True:
            try:
                event = self.event_queue.get_nowait()
            except queue.Empty:
                break
            self.endpoint_pane.handle_event(event)
        self.after(100, self._drain_events)

    def on_close(self) -> None:
        self.stop_all()
        self.after(200, self._shutdown)

    def _shutdown(self) -> None:
        self.loop_thread.stop()
        self.destroy()


def main() -> None:
    app = SimulatorApp()
    app.mainloop()


if __name__ == "__main__":
    main()
