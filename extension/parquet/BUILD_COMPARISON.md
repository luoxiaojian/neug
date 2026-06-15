# Parquet 扩展构建对比：Carquet vs Arrow

本文档记录在启用 `parquet` 扩展时，`PARQUET_USE_ARROW=OFF`（Carquet / Native）与 `PARQUET_USE_ARROW=ON`（Arrow / libparquet）两种后端在**从零冷启动构建**下的耗时与产物体积差异。

## 测试环境

| 项目 | 值 |
|------|-----|
| 日期 | 2026-06-15 |
| 平台 | macOS arm64 |
| 构建类型 | `Release` |
| 并行度 | `sysctl -n hw.ncpu`（物理核） |
| 测量方式 | 空 build 目录，`/usr/bin/time -p` 统计 configure + build 总墙钟时间 |

## 共同 CMake 参数

两种模式除 `PARQUET_USE_ARROW` 外其余选项一致：

```bash
cmake -S . -B <build-dir> \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_PYTHON=OFF \
  -DBUILD_TEST=OFF \
  -DBUILD_EXTENSIONS="parquet"
```

> **注意**：不要同时启用 `httpfs`。`httpfs` 会强制拉取 Arrow，无法单独对比 parquet 后端的差异。

### Carquet（默认，不引入 Arrow）

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_PYTHON=OFF \
  -DBUILD_EXTENSIONS="parquet" \
  -DPARQUET_USE_ARROW=OFF

cmake --build build -j$(sysctl -n hw.ncpu)
```

### Arrow（libparquet 后端）

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_PYTHON=OFF \
  -DBUILD_EXTENSIONS="parquet" \
  -DPARQUET_USE_ARROW=ON

cmake --build build -j$(sysctl -n hw.ncpu)
```

## 编译耗时

| 模式 | 总耗时 | 相对 Carquet |
|------|--------|--------------|
| **Carquet** (`PARQUET_USE_ARROW=OFF`) | **6.1 分钟**（363 s） | 1.0× |
| **Arrow** (`PARQUET_USE_ARROW=ON`) | **17.3 分钟**（1035 s） | **2.8×** |

Arrow 模式额外耗时主要来自：

- 下载并编译 **Apache Arrow 18**（NeuG 使用 `graphscope.oss-cn-beijing.aliyuncs.com` 国内镜像拉取 Arrow 源码）
- FetchContent **Boost headers**（`boost_ep`，供 Thrift 编译使用）
- 编译 **Apache Thrift**（`thrift_ep`）
- 编译 snappy / zlib / zstd 等 bundled 压缩库

Carquet 路径仅 FetchContent Carquet 及其 lz4/zstd 依赖，第三方体量显著更小。

## 构建目录占用

含源码、中间文件、静态库等**整个 build 树**的大小：

| 项目 | Carquet | Arrow | 倍数 |
|------|---------|-------|------|
| 整个 `build/` 目录 | 387 MB | 710 MB | 1.8× |
| `_deps/` 第三方 | 93 MB | 399 MB | 4.3× |

Arrow 模式下 `_deps/arrow-build/` 约占 294 MB，其中 Boost/Thrift 的中间产物占相当比例（本次测量：`boost_ep-prefix` ~136 MB，`thrift_ep-prefix` ~48 MB）。

## 运行时产物大小

真正部署时需要关注的动态库（macOS 实测）：

| 产物 | Carquet | Arrow | 说明 |
|------|---------|-------|------|
| `src/libneug.dylib` | 26.0 MB | 26.0 MB | 相同（核心库与 parquet 后端无关） |
| `extension/parquet/libparquet.neug_extension` | **1.40 MB** | **14.54 MB** | 扩展动态库 |
| **合计（neug + extension）** | **27.4 MB** | **40.6 MB** | Arrow 多约 **13 MB（+48%）** |

扩展体积：**Arrow 约为 Carquet 的 10.4×**。

### 链入扩展的后端静态库（参考）

| 后端 | 主要静态库 | 合计 |
|------|-----------|------|
| Carquet | `libcarquet.a` | ~0.8 MB |
| Arrow | `libarrow.a`、`libarrow_bundled_dependencies.a`、`libthrift.a`、snappy/zstd/zlib 等 | ~26 MB |

Arrow 模式下 `libparquet` 以 object library 形式链入 extension，通常不单独产出 `libparquet.a`，体积已体现在 `libparquet.neug_extension` 中。

## 功能差异

| 能力 | Carquet (`OFF`) | Arrow (`ON`) |
|------|-----------------|--------------|
| Parquet **读取** | ✅ | ✅ |
| Parquet **导出**（`COPY_PARQUET` 等） | ❌ | ✅ |
| 运行时依赖 Thrift / Boost | ❌ | 编译期依赖 Thrift；Boost 多为 headers |
| 默认 `PARQUET_BACKEND` | `native` | `arrow` |
| 可选运行时切换后端 | 无 Arrow 编译时强制 Native | 支持 `arrow` / `native` / `auto` |

## Arrow 依赖说明（简要）

- **Thrift 运行时**：Arrow 读 Parquet **无法去掉**。Parquet footer 元数据为 Thrift 序列化；Arrow 仓库已包含预生成代码，**不需要 Thrift 编译器**，但仍需链接 `libthrift`。
- **Boost**：Parquet-only 精简构建通常只需 **Boost headers**（不编完整 Boost 库），但 bundled 构建仍会下载 Boost 源码包。
- **`PARQUET_MINIMAL_DEPENDENCY`**：历史 CMake 选项，已在 Arrow 上游废弃，不能作为「无 Thrift 读 Parquet」的方案。

若目标是轻量化 **只读** Parquet，优先使用 Carquet；需要 **Export** 或 Arrow 生态能力时再启用 Arrow。

## 构建产物位置

标准构建树为仓库根目录下的 **`build/`**（见 `AGENTS.md`）。不要在仓库根目录随意创建 `build-*` 临时目录；若需自定义路径，使用 `NEUG_BUILD_DIR`。

```
build/
├── src/libneug.dylib
├── extension/parquet/libparquet.neug_extension
└── _deps/
    ├── carquet-build/          # PARQUET_USE_ARROW=OFF
    └── arrow-build/            # PARQUET_USE_ARROW=ON
```

## 复现 benchmark

本次对比使用的独立构建目录（可删除）：

```bash
# Carquet benchmark
cmake -S . -B bench-build-carquet \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_PYTHON=OFF -DBUILD_TEST=OFF \
  -DBUILD_EXTENSIONS="parquet" -DPARQUET_USE_ARROW=OFF
/usr/bin/time -p cmake --build bench-build-carquet -j$(sysctl -n hw.ncpu)

# Arrow benchmark
cmake -S . -B bench-build-arrow \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_PYTHON=OFF -DBUILD_TEST=OFF \
  -DBUILD_EXTENSIONS="parquet" -DPARQUET_USE_ARROW=ON
/usr/bin/time -p cmake --build bench-build-arrow -j$(sysctl -n hw.ncpu)
```

清理 benchmark 目录：

```bash
rm -rf bench-build-carquet bench-build-arrow
```

## 结论摘要

| 维度 | Carquet | Arrow |
|------|---------|-------|
| 从零编译耗时 | ~6 min | ~17 min（2.8×） |
| 扩展 `.neug_extension` | ~1.4 MB | ~14.5 MB（10×） |
| 部署体积（neug + ext） | ~27 MB | ~41 MB |
| build 树磁盘 | ~387 MB | ~710 MB |

**推荐**：默认 `PARQUET_USE_ARROW=OFF`（Carquet）；仅在需要 Parquet Export 或 Arrow 后端特性时设为 `ON`。
