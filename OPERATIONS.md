# 分段湖表作业运行约定

## 已有系统及真实运行条件
Python 3.9+ 标准库、SQLite；目标为 macOS 13+ 普通登录用户，无管理员权限、联网依赖或额外服务。安装位置、数据目录和解释器路径可能含空格与中文。解释器由运维人员提供，必须以绝对路径记录；不捆绑解释器。发布后源码目录会移走，运行目录不等于源码目录。不要使用 Docker 或容器配置。

pipeline.py 的 validate、normalize、merge、summarize、encoded、digest 是已验收业务核，保持其行为。Engine 和 LocalBlobs 的持久化编排允许调整，也可增加运维元数据。保留 jobs 表现有列与检查点形状，以及 blobs/<sha256>.json 的已发布载荷格式，已有运行目录应可继续处理。

每批是独立入湖成果。输入为 UTF-8 JSON：
{"batch":"批次ID","segments":[[{"id":"样本ID","rev":1,"sensor":"站点","ts":61,"value":120,"deleted":false}]]}
批次含1～16段，每段0～2000行。字符串非空；rev为正整数；ts为整数秒，value为整数计量值，deleted为布尔。每个样本取最高rev，允许换站、改时间、撤销和复活；同版异内容拒绝整批。分钟为ts//60，未删除样本按(sensor,minute)汇总sum/count，负值和零保留。批次ID重放相同内容返回相同接纳回执，异内容为BATCH_CONFLICT，不能改账。

python3 manual.py submit ROOT samples/a.json 接纳a，step ROOT推进一次检查点，drain ROOT处理所有待办，read ROOT读取完成结果，status ROOT展示检查点和现存载荷。
a的rows恰为[{"sensor":"s1","minute":-1,"sum":7,"count":1},{"sensor":"s2","minute":1,"sum":-30,"count":2}]。
b的rows恰为[{"sensor":"s1","minute":0,"sum":100,"count":1}]。
read的列表按batch排序，每项含batch、rows、receipt；receipt含batch、sha256、groups、samples，以pipeline.py为准。结果和完成回执一起提交；未完成批次不出现在read中。接纳回执为{"batch":ID,"sha256":规范输入摘要}，已完成后重放仍相同。
checks.py normal为纯内存业务回归，disk另做真实落盘重开，demo输出真实业务结果和当前未清理载荷数。原断言保留。

## 本轮固定运维入口
源码入口为python3 ops.py，成功stdout为一个JSON值，错误退出非零并给出原因。以下OUT、PACKAGE、DEST、ROOT均为路径，ABS为解释器绝对路径：
- build OUT：OUT内产生app.pyz和manifest.json，后者为{"sha256":"app.pyz的SHA-256"}；输出{"package":OUT的绝对路径}。包应包含运行所需代码，不依赖开发目录或当前目录。
- install PACKAGE DEST --python ABS [--no-load]：验证包后安装DEST/app.pyz，使用DEST/data作为ROOT，返回{"app":绝对路径,"root":绝对路径,"plist":绝对路径,"label":LaunchAgent标签}。损坏包不得替换旧包或旧启动配置。本题不要求运行中升级或多版本回退；安装时没有加工进程。
- submit ROOT INPUT、read ROOT：沿用manual.py的输入、输出和业务异常。
- run ROOT --once：推进一次Engine.step，输出{"progressed":布尔}；run ROOT为常驻循环，持续接纳待办。重复启动加工进程须被worker.lock拒绝。submit不占用此长驻锁，可与加工并行。
- status ROOT：保留Engine.status的pending、completed、required，增加inventory，形状为{"载荷摘要":文件字节数}，只统计已发布的.json载荷。
- clean ROOT：输出{"removed_files":["相对ROOT的路径"],"released_bytes":本次实际删除载荷的字节数}，路径排序；无删除时为[]和0。只删除失去保护的blobs载荷及遗留写临时文件，不能删除入账结果、回执、数据库、锁文件和运维元数据。
安装后的python3 DEST/app.pyz支持install、submit、run、read、status、clean，同样可从任意工作目录调用。

## 清理与恢复边界
限定同机一个加工者、一个提交者和一个清理者，可三者交错，不要求多个清理者或多个加工者。清理不能等整批完成或夺取worker.lock；也不能在载荷读写期间持有阻塞所有批次的全局协调锁，短元数据事务允许。
已接纳但未处理的输入、全部当前检查点的dependencies，以及尚在交接的活跃载荷都属于必要保护。重复内容只有一份物理载荷，删除一个批次的依赖不能伤及另一个。处理结束后的结果在SQLite内，不要求保留历史检查点载荷。
进程可在对象发布、检查点替换、完成提交、删除前后被SIGKILL；重开无需手改数据库或补回载荷。不要求断电/文件系统损坏恢复。仍活着的暂停者不是孤儿；死亡留下的保护不能永久阻止回收。失去全部引用且没有活跃持有者的载荷，在相关操作结束或进程重开后再次clean时应实际清除。
已发布载荷的名称可以被后续相同内容复用，不能凭一次过期的依赖快照直接删除。
released_bytes只计本次实际删除文件的大小，不能按逻辑引用次数重复计费；此前已删或缺失的文件不计。中断后允许再次clean，不要求恢复上一次未返回的统计回执。清理前后read逐行结果、全部原回执应相同。

## 发布与验证辅助
默认install生成RunAtLoad与KeepAlive的用户级LaunchAgent，将plist放入当前用户Library/LaunchAgents并通过launchctl bootstrap gui/<uid>加载。ProgramArguments必须为[ABS,安装app.pyz绝对路径,"run",ROOT绝对路径]，不得依赖shell激活、PATH查找或源码WorkingDirectory。日志位于DEST下。
--no-load只在DEST内生成等价plist，不写用户LaunchAgents目录、不调用launchctl，供无登录会话的回归使用；它不能代替macOS上线验证。agentcheck.py可检查plist和实际加载状态。
Gate已提供POINTWISE_PAUSE和POINTWISE_GATE；匹配事件第一次到达时写ready.json，等待同目录continue文件。流水线现有事件见源码。清理增加before_delete事件，detail含相对ROOT的path，在完成删除资格裁决后、实际删除前调用同一Gate；如果采用回收站，指向届时待删除文件的路径。
releasecheck.py是安装包上线回归辅助，包含暂停交错、共享载荷、真实SIGKILL重开和清理中断；可补测，不得以打印成功代替业务核对。最终还需覆盖完成提交前后、同名载荷重新接纳与遗留临时文件边界，并保留可重跑入口和实际输出。
在macOS中默认安装后，提交一个批次，由已加载的LaunchAgent自行完成，再用安装入口read核账；记录launchctl状态与结果，最后可bootout并移除本次测试标签。
修改.pointwise-runtime.json的verify/demo，使其验证与展示经过安装入口的真实结果；交付简短恢复说明及上线回归记录，区分实际运行和仅生成配置的验证。本题不要求界面、调度器、历史备份、存储迁移或通用文件清理工具。
