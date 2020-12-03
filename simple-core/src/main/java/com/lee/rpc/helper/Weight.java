package com.lee.rpc.helper;

public enum Weight {
    /**
     * weight
     */
    TEN(10),
    NINE(9),
    EIGHT(8),
    SEVEN(7),
    SIX(6),
    FIVE(5),
    FOUR(4),
    THREE(3),
    TWO(2),
    ONE(1);

    private final int weight;

    Weight(int weight) {
        this.weight = weight;
    }

    public int value() {
        return this.weight;
    }

    public static Weight toWeight(int weight) {
        Weight[] values = values();
        for (Weight value : values) {
            if (value.weight == weight) {
                return value;
            }
        }
        throw new IllegalArgumentException("Can not support weight " + weight);
    }
}
