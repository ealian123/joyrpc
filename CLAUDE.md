# CLAUDE.md

本文档为 Claude Code（claude.ai/code）在此仓库中工作时提供指导。

## 构建与测试

- 这是一个以 `pom.xml` 为根的多模块 Maven 项目。
- Java 版本为 1.8，由根 POM 中的 `${jdk.version}` 指定。
- 根 POM 中的 Surefire 默认配置为跳过测试（`<maven-surefire-plugin.skipTests>true</maven-surefire-plugin.skipTests>`），因此在需要实际执行测试时，请显式加上 `-Dmaven-surefire-plugin.skipTests=false`。

常用命令：

```bash
mvn clean install
mvn -DskipTests package
mvn -Dmaven-surefire-plugin.skipTests=false test
mvn -pl joyrpc-core -am test -Dmaven-surefire-plugin.skipTests=false
mvn -pl joyrpc-test/joyrpc-test-serialization -am test -Dmaven-surefire-plugin.skipTests=false
mvn -pl joyrpc-test/joyrpc-test-serialization -Dtest=SerializationTest -Dmaven-surefire-plugin.skipTests=false test
mvn -pl joyrpc-test/joyrpc-test-cluster -Dtest=RegionCandidatureTest -Dmaven-surefire-plugin.skipTests=false test
```

说明：

- `-pl <module> -am` 是构建或测试单个模块及其上游依赖的标准方式。
- `joyrpc-test` 是 JUnit 5 与 JMH 验证模块的主要位置。
- `joyrpc-all` 用于构建聚合发布产物，并使用 shade 插件打包。

## 仓库结构

根聚合模块下的顶层模块包括：

- `joyrpc-api`：供业务应用使用的公共注解与 API 类型。
- `joyrpc-core`：核心运行时，包含协议处理链路、集群逻辑、配置模型、过滤器、传输抽象以及内置实现。
- `joyrpc-plugin`：按能力划分的可选插件实现，包括 cache、codec、protocol、proxy、registry、expression、transport、trace、transaction。
- `joyrpc-extension`：扩展加载基础设施，包括面向 Spring / Spring Boot 的扩展发现集成。
- `joyrpc-spring`：基于 Spring XML / Bean 的集成层。
- `joyrpc-springboot`：Spring Boot 自动配置与注解扫描集成。
- `joyrpc-test`：面向 cache、cluster、compression、proxy、serialization、extension、quickstart 以及共享测试工具的测试模块集合。
- `joyrpc-example`：Spring、Spring Boot、Dubbo 互通、gRPC 互通等可运行示例。
- `joyrpc-all`：供发布或统一引入使用的聚合产物。

## 架构概览

JOYRPC 是一个 Java RPC 框架，整体采用异步、微内核、插件化设计，这一点在 `README.md` 中也有说明。

### 1. 通过扩展点实现微内核

核心架构思想是：绝大多数主要行为都通过扩展点选择，而不是写死在单一实现中。

- 扩展点的中心注册入口位于 `joyrpc-core/src/main/java/io/joyrpc/Plugin.java`。
- 该文件暴露了 filters、registries、protocols、serialization、compression、tracing、transactions、routing、thread pools、authentication/authorization、cache 等多个 `ExtensionPoint`。
- 具体实现通常来自 `joyrpc-plugin/*` 模块，并通过 `@Extension` 标注注册。

当你需要新增或修改框架能力时，先判断它属于：

- `joyrpc-core` 中的核心 SPI 契约，还是
- `joyrpc-plugin`、`joyrpc-spring`、`joyrpc-springboot` 下的插件实现。

### 2. 核心运行时拆分

在 `joyrpc-core` 中，关键的概念分区包括：

- `io.joyrpc.config`：consumer/provider/server/registry 等配置对象。`ConsumerConfig`、`ProviderConfig`、`AbstractInterfaceConfig` 是核心入口。
- `io.joyrpc.invoker`：服务发布与引用生命周期、调用处理、回调支持、选项计算、过滤链组装。`Refer` 是消费端运行时的重要入口。
- `io.joyrpc.protocol`：协议抽象与编解码。`Protocol` 与 `AbstractProtocol` 定义协议行为，`AbstractCodec` 处理帧与编码相关逻辑。
- `io.joyrpc.transport`：客户端/服务端传输抽象与 endpoint 工厂。具体传输实现来自 Netty、RESTEasy 等插件模块。
- `io.joyrpc.cluster`：注册发现、路由、负载均衡、故障转移、熔断、区域感知候选节点选择、节点状态管理。
- `io.joyrpc.filter`：异步 consumer/provider 过滤链，承载 tracing、validation、concurrency、authorization、generic call、cache、timeout 等横切能力。
- `io.joyrpc.context`：运行时策略与路由相关的动态配置、上下文传播钩子。

推荐使用下面这个心智模型理解整体链路：

1. 配置对象定义服务如何发布或引用；
2. invoker / runtime 代码把配置转换为 exporter / reference；
3. protocol + transport 负责消息传输；
4. cluster 逻辑负责目标选择，并响应注册中心与配置变化；
5. filters 以异步方式包裹调用链；
6. plugins 在各个扩展缝隙上提供具体实现。

### 3. 注册中心与集群模型

服务发现不是单一实现，而是另一个扩展面。

- Registry SPI 位于 `io.joyrpc.cluster.discovery.registry`。
- `joyrpc-core` 中的 `AbstractRegistry` 与 `AbstractRegistryFactory` 提供共享基础模型。
- memory、fix 这类内置或简单实现位于 core 中。
- ZK、Nacos、Etcd、Consul 等外部注册中心实现位于 `joyrpc-plugin/joyrpc-registry/*`。

路由、节点选择、负载均衡、重试、熔断等集群行为同样位于 core 中，并通过扩展点组合，而不是由一个单体式调度器统一硬编码实现。

### 4. 协议、序列化、压缩、传输是彼此独立的插件维度

这个框架显式拆分了多个在简化 RPC 框架中常常耦合在一起的关注点：

- 协议选择（`io.joyrpc.protocol.Protocol`）
- 序列化（`io.joyrpc.codec.serialization.Serialization` 及相关 codec SPI）
- 压缩
- 传输层 client/server 实现
- 代理生成

这也是 `joyrpc-plugin` 按能力族拆分，而不是按业务功能拆分的原因。例如：

- `joyrpc-plugin/joyrpc-protocol/*`：提供 HTTP、gRPC、Dubbo、telnet 等协议实现。
- `joyrpc-plugin/joyrpc-codec/*`：提供序列化与压缩实现。
- `joyrpc-plugin/joyrpc-transport/*`：提供具体传输栈实现。
- `joyrpc-plugin/joyrpc-proxy/*`：提供代理生成策略。

如果某个行为看起来像“协议相关”，修改 core 之前先确认它是否实际被拆分到了 protocol、codec、transport 多个模块里。

### 5. Spring 与 Spring Boot 集成层

这里有两层不同的集成：

- `joyrpc-spring`：传统 Spring Bean / XML 集成。关键 Bean 包装类包括 `ProviderBean`、`ConsumerBean`、`ConsumerGroupBean`、`ServerBean`、`RegistryBean`、`MethodBean`。
- `joyrpc-springboot`：Boot 自动配置与定义扫描。主要入口是 `RpcAutoConfiguration`、`RpcDefinitionPostProcessor`、`RpcProperties`。

此外，`joyrpc-extension/joyrpc-extension-spring` 提供了感知 Spring 容器的扩展加载器。`SpringLoader` 会把 Spring Bean 注册进框架扩展系统，使 Spring 管理的实现也能参与 JoyRPC 的扩展点装配。

排查 Spring 相关问题时，要区分两件事：

- RPC 服务的发布 / 引用（`joyrpc-spring` / `joyrpc-springboot`）；
- 从 Spring 容器中加载 SPI 风格扩展（`joyrpc-extension-*`）。

### 6. 测试策略

测试被有意从运行时模块中拆分出来，集中放在 `joyrpc-test/*` 子模块下。

常用测试模块包括：

- `joyrpc-test-cache`
- `joyrpc-test-cluster`
- `joyrpc-test-compress`
- `joyrpc-test-proxy`
- `joyrpc-test-serialization`
- `joyrpc-test-extension`
- `joyrpc-test-quickstart`
- `joyrpc-test-util`

当你修改某个插件族或特定子系统时，优先执行对应的 `joyrpc-test-*` 模块，而不是一开始就跑完整个 reactor。

## 已检查的仓库指导来源

- 仓库根目录 `README.md`：提供项目定位与特性概览。
- 仓库中原先不存在 `CLAUDE.md`。
- 仓库根目录下不存在 `.cursorrules`、`.cursor/rules/`、`.github/copilot-instructions.md`。
