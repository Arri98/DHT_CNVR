package es.upm.dit.dscc.DHT;

/**
 * Tipo de respuesta
 */

public enum ScannerAnswerEnum {
    /**
     * La key no existe
     */
    NO_KEY
    ,
    /**
     * Se ha de consultar la key en otro nodo
     */
    EXTERNAL_PETITION,
    /**
     * Se proporciona la respuesta
     */
    ANSWER
}
