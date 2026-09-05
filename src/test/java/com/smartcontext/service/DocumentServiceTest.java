package com.smartcontext.service;

import com.smartcontext.repository.DocumentRepository;
import org.junit.jupiter.api.Test;
import org.springframework.http.codec.multipart.FilePart;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DocumentServiceTest {

    @Test
    void rejectsUnsupportedFileType() {
        FilePart file = mock(FilePart.class);
        when(file.filename()).thenReturn("malware.exe");
        DocumentService service = new DocumentService(
                Path.of("./data/uploads"),
                mock(DocumentRepository.class),
                mock(IngestProducer.class)
        );
        assertThrows(IllegalArgumentException.class, () -> service.upload(file));
    }
}
