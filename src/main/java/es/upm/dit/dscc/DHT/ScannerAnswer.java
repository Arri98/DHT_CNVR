package es.upm.dit.dscc.DHT;

public class ScannerAnswer {


    ScannerAnswerEnum typeAnswer;
    DHT_Map map;

    public ScannerAnswer(ScannerAnswerEnum typeAnswer, DHT_Map map) {
        this.typeAnswer = typeAnswer;
        this.map = map;
    }

    public ScannerAnswerEnum getTypeAnswer() {
        return typeAnswer;
    }

    public DHT_Map getMap() {
        return map;
    }

}
