package com.geekhua.filequeue.exception;

/**
 * @author Leo Liang
 * 
 */
public class FileQueueClosedException extends Exception {

    private static final long serialVersionUID = 4135894510349835781L;

    public FileQueueClosedException() {
        super();
    }

    public FileQueueClosedException(String message) {
        super(message);
    }

    public FileQueueClosedException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileQueueClosedException(Throwable cause) {
        super(cause);
    }
}
