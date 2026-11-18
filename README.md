# Actividad 3.2.2 - Pipeline de eventos bancarios end-to-end

Implementa el caso de la guía con Spring Boot y Kafka Streams.

```text
POST /api/banco/transaccion
  -> banco-transacciones (key = usuario)
  -> filtro: monto > 10 USD
  -> agregación: total acumulado por usuario (state store)
  -> detección: monto > 10.000 USD
  -> banco-alertas
```

La conversión a USD es simulada: los montos de entrada se tratan como USD, tal como el contrato de la guía no incluye moneda de origen. La key `usuario` preserva el orden de cada usuario dentro de su partición.

## Ejecutar

Requiere Docker Desktop, Java 17 y Maven.

```powershell
docker compose up -d
mvn spring-boot:run
```

Kafka UI queda en <http://localhost:8080> y la API usa <http://localhost:8081> para no colisionar con ella.

## Pruebas

Transacción normal: se procesa y suma al total, sin alerta.

```powershell
curl.exe -X POST "http://localhost:8081/api/banco/transaccion" -H "Content-Type: application/json" -d '{"usuario":"juan","monto":500,"origen":"app"}'
```

Transacción fraudulenta: además genera una alerta en `banco-alertas`.

```powershell
curl.exe -X POST "http://localhost:8081/api/banco/transaccion" -H "Content-Type: application/json" -d '{"usuario":"juan","monto":15000,"origen":"cajero"}'
```

Transacción filtrada: no llega a la agregación ni genera alerta.

```powershell
curl.exe -X POST "http://localhost:8081/api/banco/transaccion" -H "Content-Type: application/json" -d '{"usuario":"juan","monto":5,"origen":"app"}'
```

En Kafka UI, abre **Topics > banco-alertas > Messages**. Para la segunda prueba debe aparecer un mensaje como:

```json
{
  "usuario": "juan",
  "monto": 15000.0,
  "motivo": "Transaccion inusualmente alta en cajero",
  "severidad": "ALTA"
}
```

Para detener la infraestructura:

```powershell
docker compose down
```
