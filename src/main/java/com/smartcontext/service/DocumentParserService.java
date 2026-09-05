package com.smartcontext.service;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@Service
public class DocumentParserService {

    public Mono<String> extractText(Path file, String filename, String contentType) {
        return Mono.fromCallable(() -> readFile(file, filename, contentType))
                .subscribeOn(Schedulers.boundedElastic());
    }

    public List<String> chunk(String text, int chunkSize, int overlap) {
        String normalized = text == null ? "" : text.replace("\r\n", "\n").trim();
        if (normalized.isBlank()) {
            return List.of();
        }
        List<String> chunks = new ArrayList<>();
        int start = 0;
        while (start < normalized.length()) {
            int end = Math.min(normalized.length(), start + chunkSize);
            String piece = normalized.substring(start, end).trim();
            if (!piece.isBlank()) {
                chunks.add(piece);
            }
            if (end >= normalized.length()) {
                break;
            }
            start = end - overlap;
        }
        return chunks;
    }

    private String readFile(Path file, String filename, String contentType) throws Exception {
        String lower = filename == null ? "" : filename.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".pdf") || MediaType.APPLICATION_PDF_VALUE.equals(contentType)) {
            try (PDDocument pdf = PDDocument.load(file.toFile())) {
                return new PDFTextStripper().getText(pdf);
            }
        }
        return Files.readString(file, StandardCharsets.UTF_8);
    }
}
