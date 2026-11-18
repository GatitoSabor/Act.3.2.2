package com.banco.controller;

import com.banco.config.KafkaTopicsConfig;
import com.banco.model.Transaccion;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/banco")
public class SimuladorController {
    private final KafkaTemplate<String, Transaccion> kafkaTemplate;

    public SimuladorController(KafkaTemplate<String, Transaccion> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/transaccion")
    public ResponseEntity<String> enviarTransaccion(@RequestBody Transaccion transaccion) {
        if (transaccion.getUsuario() == null || transaccion.getUsuario().isBlank()
                || transaccion.getMonto() == null || transaccion.getOrigen() == null || transaccion.getOrigen().isBlank()) {
            return ResponseEntity.badRequest().body("usuario, monto y origen son obligatorios");
        }
        if (transaccion.getTimestamp() == null) {
            transaccion.setTimestamp(System.currentTimeMillis());
        }

        kafkaTemplate.send(KafkaTopicsConfig.TRANSACCIONES, transaccion.getUsuario(), transaccion);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(
                "Transaccion enviada: " + transaccion.getMonto() + " de " + transaccion.getUsuario());
    }
}
