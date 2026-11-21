#pragma once
#include "aggregate.hpp"
#include <chrono>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

template<class T> T checked(arrow::Result<T> result) {
    if (!result.ok()) throw std::runtime_error(result.status().ToString());
    return std::move(result).ValueUnsafe();
}

inline void checked(arrow::Status status) {
    if (!status.ok()) throw std::runtime_error(status.ToString());
}

template<class... Builders> class ParquetOutput {
    std::tuple<Builders...> columns;
    std::unique_ptr<parquet::arrow::FileWriter> writer;
    int rows = 0;
    using Clock = std::chrono::steady_clock;
    Clock::duration writing{};

    void flush() {
        if (!rows) return;
        const auto start = Clock::now();
        auto batch = std::apply([&](auto&... column) {
            return arrow::RecordBatch::Make(writer->schema(), rows, {checked(column.Finish())...});
        }, columns);
        checked(writer->WriteRecordBatch(*batch));
        rows = 0;
        writing += Clock::now() - start;
    }

    template<class Builder, class T> static void append(Builder& column, T value) {
        checked(column.Append(value));
    }

    static void append(arrow::StringBuilder& column, const std::string* value) {
        checked(value ? column.Append(*value) : column.AppendNull());
    }

public:
    ParquetOutput(const std::filesystem::path& path, std::initializer_list<const char*> names) {
        const auto start = Clock::now();
        std::vector<std::shared_ptr<arrow::Field>> fields;
        auto name = names.begin();
        std::apply([&](auto&... column) {
            (fields.push_back(arrow::field(*name++, column.type())), ...);
        }, columns);
        auto stream = checked(arrow::io::FileOutputStream::Open(path.string()));
        parquet::WriterProperties::Builder properties;
        properties.compression(parquet::Compression::SNAPPY);
        writer = checked(parquet::arrow::FileWriter::Open(*arrow::schema(std::move(fields)),
                         arrow::default_memory_pool(), stream, properties.build()));
        writing += Clock::now() - start;
    }

    template<class... Values> void add(Values... values) {
        std::apply([&](auto&... column) { (append(column, values), ...); }, columns);
        if (++rows == 65536) flush();
    }

    void close() {
        flush();
        const auto start = Clock::now();
        checked(writer->Close());
        writing += Clock::now() - start;
    }

    Clock::duration write_time() const { return writing; }
};

using UserOutput = ParquetOutput<arrow::StringBuilder, arrow::Date32Builder, arrow::Int64Builder,
                                 arrow::Int64Builder, arrow::Int64Builder, arrow::StringBuilder>;
using CampaignOutput = ParquetOutput<arrow::StringBuilder, arrow::StringBuilder, arrow::Date32Builder,
                                     arrow::Int64Builder, arrow::Int64Builder, arrow::Int64Builder,
                                     arrow::Int64Builder>;
