# jmqx-parent — Maven Parent POM

Jmqx 项目的 Maven 父 POM，集中管理全局依赖版本、插件配置和 Maven Central 发布流程。

## 职责

- **依赖管理**：使用 `dependencyManagement` 统一管控所有子模块的第三方依赖版本（Reactor Netty、Netty、Jackson、ScaleCube、Lombok、SLF4J、Hutool 等）
- **版本号统一**：通过 `revision` 属性（CI-Friendly）实现所有模块版本号一致
- **构建配置**：配置 `maven-compiler-plugin`（JDK 17）、`flatten-maven-plugin`（发布扁平化 POM）
- **Maven Central 发布**：`nexus-central-release` Profile 配置了 source/javadoc 打包、GPG 签名和 Sonatype Central 发布插件

## 依赖版本

| 依赖 | 版本 |
|---|---|
| Reactor BOM | 2024.0.4 |
| Netty | 4.1.119.Final |
| ScaleCube Cluster | 2.6.17 |
| Jackson BOM | 2.11.0 |
| Lombok | 1.18.30 |
| SLF4J | 1.7.30 |
| Hutool | 5.8.35 |

## 使用

所有子模块以 `plus.jmqx.iot:jmqx-parent` 为父 POM：

```xml
<parent>
    <groupId>plus.jmqx.iot</groupId>
    <artifactId>jmqx-parent</artifactId>
    <version>${revision}</version>
    <relativePath>../jmqx-parent</relativePath>
</parent>
```

## 发布到 Maven Central

激活 `nexus-central-release` Profile：

```shell
mvn clean deploy -P nexus-central-release
```

该 Profile 会自动执行源码打包、Javadoc 打包、GPG 签名和 Sonatype Central 发布。