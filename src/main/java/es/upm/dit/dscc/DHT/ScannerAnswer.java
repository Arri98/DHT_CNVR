package es.upm.dit.dscc.DHT;

/**
 * Respuesta a una peticion del scanner
 */

public class ScannerAnswer {

    ScannerAnswerEnum typeAnswer;
    DHT_Map map;

    /**
     * Constructor
     * @param typeAnswer Tipo de respuesta
     * @param map Respuesta
     */

    public ScannerAnswer(ScannerAnswerEnum typeAnswer, DHT_Map map) {
        this.typeAnswer = typeAnswer;
        this.map = map;
    }

    /**
     * Lee el tipo de respuesta
     * @return Elemento del enum, que representa el tipo de respuesta
     */

    public ScannerAnswerEnum getTypeAnswer() {
        return typeAnswer;
    }


    /**
     * Respuesta
     * @return Map key value
     */
    public DHT_Map getMap() {
        return map;
    }

}
