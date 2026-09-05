package com.smartcontext.service;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DocumentParserServiceTest {

    private final DocumentParserService parser = new DocumentParserService();

    @Test
    void chunkSplitsWithOverlap() {
        String text = "a".repeat(2500);
        List<String> chunks = parser.chunk(text, 1200, 200);
        assertEquals(3, chunks.size());
        assertEquals(1200, chunks.get(0).length());
        assertEquals("a".repeat(200), chunks.get(1).substring(0, 200));
    }

    @Test
    void chunkReturnsEmptyForBlank() {
        assertTrue(parser.chunk("   ", 1200, 200).isEmpty());
        assertTrue(parser.chunk(null, 1200, 200).isEmpty());
    }

    @Test
    void chunkKeepsShortTextAsOnePiece() {
        List<String> chunks = parser.chunk("hello world", 1200, 200);
        assertEquals(List.of("hello world"), chunks);
    }
}
