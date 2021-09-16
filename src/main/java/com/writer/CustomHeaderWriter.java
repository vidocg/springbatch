package com.writer;

import org.springframework.batch.item.file.FlatFileItemWriter;

public class CustomHeaderWriter<T> extends FlatFileItemWriter<T> {
    public CustomHeaderWriter(String... headers) {
        super.setHeaderCallback(writer -> writer.write(concatHeaders(headers)));
    }

    private String concatHeaders(String[] headers) {
        return String.join(",", headers);
    }
}
