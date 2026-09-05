package com.smartcontext.controller;

import com.smartcontext.service.DocumentService.NotFoundException;
import org.springframework.beans.TypeMismatchException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ServerWebInputException;

import java.util.Map;
import java.util.UUID;

@RestControllerAdvice
public class RestExceptionHandler {

    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<Map<String, String>> notFound(NotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", e.getMessage() == null ? "not found" : e.getMessage()));
    }

    @ExceptionHandler({IllegalArgumentException.class, ServerWebInputException.class, TypeMismatchException.class})
    public ResponseEntity<Map<String, String>> badRequest(Exception e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", uuidMessage(e)));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, String>> serverError(Exception e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", root.getMessage() == null ? e.getClass().getSimpleName() : root.getMessage()));
    }

    private String uuidMessage(Exception e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String msg = root.getMessage() == null ? e.getMessage() : root.getMessage();
        if (msg != null && msg.toLowerCase().contains("uuid")) {
            return "document id must be a UUID (36 characters, no braces)";
        }
        if (root instanceof TypeMismatchException t && UUID.class.equals(t.getRequiredType())) {
            return "document id must be a UUID (36 characters, no braces)";
        }
        return msg == null ? "bad request" : msg;
    }
}
