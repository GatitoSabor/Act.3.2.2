package com.banco.model;

public class Transaccion {
    private String usuario;
    private Double monto;
    private String origen;
    private Long timestamp;

    public Transaccion() {
    }

    public Transaccion(String usuario, Double monto, String origen, Long timestamp) {
        this.usuario = usuario;
        this.monto = monto;
        this.origen = origen;
        this.timestamp = timestamp;
    }

    public String getUsuario() { return usuario; }
    public void setUsuario(String usuario) { this.usuario = usuario; }
    public Double getMonto() { return monto; }
    public void setMonto(Double monto) { this.monto = monto; }
    public String getOrigen() { return origen; }
    public void setOrigen(String origen) { this.origen = origen; }
    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
}
