package edu.case.kvstore.readonly.rpc;

public enum WorkerState {
    IDLE(0, "IDLE"), INIT(1, "INIT"),
    WORKING(2, "WORKING"), ERROR(3, "ERROR");
    public final int stateValue;
    private final String str;

    private WorkerState(int stateValue, String str) {
        this.stateValue = stateValue;
        this.str = str;
    }

    @Override
    public String toString() {
        return this.str;
    }
}
