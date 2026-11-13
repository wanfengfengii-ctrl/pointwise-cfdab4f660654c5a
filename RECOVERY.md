# 湖表分段入湖常驻包：恢复说明与上线回归记录

本说明面向值班运维，覆盖安装包校验、常驻自启动、崩溃/终止后的自动恢复，以及上线回归如何重跑。
所有入口仅用 Python 3.9+ 标准库；解释器以绝对路径传入，不捆绑、不依赖 PATH。

## 1. 交付物与入口

- 打包：`python3 ops.py build OUT` → `OUT/app.pyz` + `OUT/manifest.json`（manifest 仅含 app.pyz 的 SHA-256）。
- 安装：`python3 ops.py install OUT DEST --python /abs/python3 [--no-load]`
  - 安装 `DEST/app.pyz`，数据根固定为 `DEST/data`（ROOT）。
  - 默认在 `~/Library/LaunchAgents/<label>.plist` 生成 RunAtLoad + KeepAlive 的用户级 LaunchAgent，
    并 `launchctl bootstrap gui/<uid>` 加载；`ProgramArguments` 为
    `[解释器绝对路径, app.pyz 绝对路径, "run", ROOT 绝对路径]`，无 shell、无 WorkingDirectory 依赖。
  - `--no-load` 只在 DEST 内生成等价 plist，不写用户目录、不调 launchctl（供无登录会话的回归）。
  - 安装前先校验 manifest 摘要、ZIP 完整性与必需模块，再做一次真实启动冒烟；任何失败都在替换前拒绝，
    旧 app.pyz 与旧 plist 字节不变。
- 安装后六个命令均可从任意工作目录调用：
  `python3 DEST/app.pyz submit|run|read|status|clean ROOT ...`，`run ROOT [--once]`。
- 开发目录移走后不影响运行：pyz 自带 `pipeline.py`、`localio.py`、`ops.py`；
  甚至可以用已安装的 app.pyz 再次执行 `build` 生成等价包。
- 成功输出单个 JSON 值；业务/包错误以非零退出并在 stderr 给出原因（如 `PACKAGE_HASH`、`WORKER_BUSY`、`BATCH_CONFLICT`）。

## 2. 磁盘布局（ROOT = DEST/data）

| 路径 | 作用 | clean 是否可删 |
| --- | --- | --- |
| `state.sqlite`（WAL） | jobs 表：请求摘要、当前检查点、入账结果、完成回执 | 否 |
| `blobs/<sha256>.json` | 已发布不可变载荷，同内容全批次共享一份 | 仅失去全部引用时 |
| `blobs/.writing-<holder>-*` | 载荷发布临时文件 | 仅无活跃持有者时 |
| `holders/<token>.lock` / `.json` | 活跃交接登记，flock 存活证明 | 死进程登记由 clean 自动回收 |
| `worker.lock` | 单加工者长驻锁（submit 不占用） | 否 |

## 3. 清理与共享引用语义

- 同时只允许一个加工者、一个提交者、一个清理者交错运行。清理者不取 `worker.lock`，
  不等整批结束；全程没有跨载荷读写的全局协调锁，只在删除前做短元数据快照（SQLite 短读）。
- 受保护集合 = 所有未完成检查点的 `dependencies` ∪ 所有活跃 holder 登记的 oid；
  holder 覆盖接纳发布、每次检查点替换与完成提交的完整交接窗口。
- holder 以独占 `flock` 证明存活：进程被 SIGKILL 后内核自动释放锁，下一次 clean 判定为死登记并回收，
  不会永久阻止回收（无“历史避险”目录或保留副本）。
- 每个文件删除流程：裁决资格 → 触发 `before_delete` Gate（detail 为相对 ROOT 的 path）→
  醒来后**重新快照**再裁决一次 → 实际 unlink。因此清理暂停期间同名载荷被重新接纳/复用时不会误删。
- `released_bytes` 只统计本次真正 unlink 的现存文件字节；已删/缺失文件不计费，不按逻辑引用次数重复计费。
- clean 永不触碰数据库、结果、回执、锁文件与运维元数据；清理前后 `read` 逐行结果与全部回执不变。

## 4. 终止后的恢复（无需手改数据库或补载荷）

| 终止位置 | 磁盘上的状态 | 重开行为 |
| --- | --- | --- |
| 载荷发布中（`before_object_publish`） | 检查点尚未引用新 oid，可能遗留 `.writing-*` | 重跑同一 step 重新发布；clean 回收孤儿临时文件 |
| 检查点替换前（`before_checkpoint`） | 旧检查点仍完整、其依赖仍受保护 | 从旧检查点继续，结果与回执不变 |
| 接纳事务前（`before_accept`） | 无 jobs 行；已发布源载荷由 holder 保护，进程死后变不可达 | 重新 submit 得到相同接纳回执，clean 回收不可达载荷 |
| 完成提交前（`before_finish`） | 批次仍 pending，acc 载荷受检查点保护，结果未入账 | 续跑后一次性写入结果+回执，未完成批次不出现在 read |
| 删除裁决后/删除中（`before_delete` 之后） | 部分文件可能已删，均为已确认无引用者 | 再次 clean 只删仍存活且仍无引用者；不补发上次统计 |

操作入口恢复命令（任意工作目录）：

```
python3 DEST/app.pyz status DEST/data     # 看 pending/completed/required/inventory
python3 DEST/app.pyz run DEST/data        # 常驻；LaunchAgent 会自动拉起，也可手工先 --once 推进一步
python3 DEST/app.pyz clean DEST/data      # 回收不可达载荷与孤儿临时文件
python3 DEST/app.pyz read DEST/data       # 核账：只含已完成批次，按 batch 排序
```

LaunchAgent 层面：

```
launchctl print gui/$(id -u)/<label>      # 查看常驻状态（label 见 install 返回）
launchctl bootout gui/$(id -u)/<label>    # 停用
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/<label>.plist   # 重新启用
```

## 5. 上线回归（可重跑入口）

| 命令 | 性质 | 覆盖 |
| --- | --- | --- |
| `python3 checks.py normal` | 纯内存业务回归 | 修订归并、分钟汇总、墓碑、共享载荷、续跑、回执、冲突拒绝 |
| `python3 checks.py disk` | 真实落盘重开 | SQLite + blobs 关闭重开后 drain 核账 |
| `python3 releasecheck.py` | **安装入口黑盒回归** | 见下 |
| `python3 releasecheck.py demo` | 同上并输出真实 JSON 证据 | 结果、清理回执、最终 inventory |
| `python3 agentcheck.py plist app root python [--live]` | plist 形状/launchd 实况 | ProgramArguments、RunAtLoad、KeepAlive、绝对路径 |

`releasecheck.py` 在临时区构建带中文/空格路径的安装目录，删除源码副本后全部经 `app.pyz` 操作，覆盖：

1. 坏包（追加字节）与坏 manifest 在替换前拒绝，已装 app 字节不变；
2. 加工者暂停在 `object_ready` 时并行 clean 必须立刻返回且不得删活跃载荷（交错清理）；
3. 两批共享同内容载荷，单批完成后共享件保留，全部引用解除后 clean 才实际删除（共享引用）；
4. 真实 SIGKILL：检查点替换后、`before_delete` 暂停后杀死，重开续跑，read/回执不变（终止续跑）；
5. `before_finish` 杀死：批次保持 pending，续跑后结果与接纳回执一致；
6. 同名载荷重新接纳：cleaner 停在 `before_delete` 期间重新接纳同内容批次，复发布存活；
7. 写临时文件边界：`before_object_publish` 杀死留下 `.writing-*`，clean 精确回收，再提交成功；
8. `worker.lock` 拒绝第二个加工者，同时 submit 不持锁可并行；
9. 二次 clean 为 `{"removed_files":[],"released_bytes":0}`，holders 无残留登记。

### 本次实际运行记录（Linux 回归机，Python 3.11.2，2026-11-13）

- `checks.py normal` → `NORMAL_REGRESSION_OK`
- `checks.py disk` → `DISK_RESUME_OK`
- `releasecheck.py` → `INSTALLED_RELEASE_REGRESSION_OK`（可重复重跑）
- `releasecheck.py demo` → 9 个批次（a–i）全部核账通过，最终 `inventory` 为空；
  证据输出含 `first_clean`、`last_clean`、`temp_clean`（真实删除回执，路径相对 ROOT、已排序）。
- 手工补充：常驻 `run` 由提交后自动加工，SIGTERM 零码退出输出 `{"stopped": true}`，再启动正常；
  SIGKILL 于 `before_accept / before_checkpoint / before_object_publish / before_finish` 四处，
  重开后结果与回执逐字一致，孤儿临时文件与死 holder 登记被后续 clean 清空。
- `agentcheck.py`（无 `--live`）通过 plist 形状检查。

### 待 macOS 上线时补做（本机为 Linux，launchd 不可用，属仅生成配置的验证）

1. 默认不带 `--no-load` 安装，确认 `~/Library/LaunchAgents/<label>.plist` 已加载：
   `python3 agentcheck.py <plist> <app> <root> $(which python3) --live`；
2. `python3 DEST/app.pyz submit DEST/data samples/a.json`，等待 LaunchAgent 自行加工完成；
3. `python3 DEST/app.pyz read DEST/data` 核账，记录 `launchctl print gui/<uid>/<label>` 状态与结果；
4. 测试结束可 `launchctl bootout gui/<uid>/<label>` 并移除本次测试标签的 plist。
