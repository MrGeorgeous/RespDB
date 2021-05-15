package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

/**
 * Зафейленная команда
 */
public class FailedDatabaseCommandResult implements DatabaseCommandResult {

    private byte[] message;

    public FailedDatabaseCommandResult(String payload) {
        if (payload == null) {
            this.message = null;
        } else {
            this.message = payload.getBytes();
        }
    }

    /**
     * Сообщение об ошибке
     */
    @Override
    public String getPayLoad() {
        if (this.message == null) {
            return null;
        } else {
            return new String(message);
        }
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    /**
     * Сериализуется в {@link RespError}
     */
    @Override
    public RespObject serialize() {
        return new RespError(message);
    }
}
