package tech.hidetora.blazegraphdemo.controller;

import lombok.RequiredArgsConstructor;
import org.openrdf.OpenRDFException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tech.hidetora.blazegraphdemo.entity.Student;
import tech.hidetora.blazegraphdemo.service.StudentService;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/api/v1/student")
@RequiredArgsConstructor
public class StudentController {
    private final StudentService studentService;

    @PostMapping
    public ResponseEntity<String> saveStudent() throws OpenRDFException, IOException {
        String serviceStudent = studentService.createStudent();
        return ResponseEntity.ok(serviceStudent);
    }

    @GetMapping
    public ResponseEntity<?> getAllStudent() throws OpenRDFException, IOException {
        List<Student> allStudent = studentService.getAllStudent();
        return ResponseEntity.ok(allStudent);
    }
}
