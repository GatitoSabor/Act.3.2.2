package com.banco.config;

import com.banco.model.AlertaFraude;
import com.banco.model.Transaccion;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class PipelineTopology {
    private static final double MONTO_MINIMO = 10.0;
    private static final double UMBRAL_FRAUDE = 10_000.0;

    @Bean
    public KStream<String, Transaccion> bancoPipeline(StreamsBuilder builder) {
        JsonSerde<Transaccion> transaccionSerde = new JsonSerde<>(Transaccion.class);
        JsonSerde<AlertaFraude> alertaSerde = new JsonSerde<>(AlertaFraude.class);

        KStream<String, Transaccion> entrada = builder.stream(
                KafkaTopicsConfig.TRANSACCIONES,
                Consumed.with(Serdes.String(), transaccionSerde));

        KStream<String, Transaccion> transaccionesValidas = entrada
                .filter((usuario, tx) -> tx != null && tx.getMonto() != null && tx.getMonto() > MONTO_MINIMO)
                .peek((usuario, tx) -> System.out.printf(
                        "Transaccion procesada | usuario=%s, monto=%.2f USD, origen=%s%n",
                        usuario, tx.getMonto(), tx.getOrigen()));

        // La key es el usuario; conserva el orden de sus eventos y permite sumar su gasto en tiempo real.
        transaccionesValidas
                .groupByKey(Grouped.with(Serdes.String(), transaccionSerde))
                .aggregate(
                        () -> 0.0,
                        (usuario, tx, totalAnterior) -> totalAnterior + tx.getMonto(),
                        Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("totales-por-usuario")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double()))
                .toStream()
                .peek((usuario, total) -> System.out.printf(
                        "Total acumulado | usuario=%s, total=%.2f USD%n", usuario, total));

        KStream<String, AlertaFraude> alertas = transaccionesValidas
                .filter((usuario, tx) -> tx.getMonto() > UMBRAL_FRAUDE)
                .mapValues(tx -> new AlertaFraude(
                        tx.getUsuario(),
                        tx.getMonto(),
                        "Transaccion inusualmente alta en " + tx.getOrigen(),
                        "ALTA"));

        alertas
                .peek((usuario, alerta) -> System.out.printf("ALERTA GENERADA | %s%n", alerta.getMotivo()))
                .to(KafkaTopicsConfig.ALERTAS, Produced.with(Serdes.String(), alertaSerde));

        return entrada;
    }
}
