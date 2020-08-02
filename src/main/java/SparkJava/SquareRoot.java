package SparkJava;

public class SquareRoot {

    private final int inputNumber;
    private final double squareRoot;

    public SquareRoot(int i) {
        this.inputNumber = i;
        this.squareRoot = Math.sqrt(inputNumber);
    }

}
