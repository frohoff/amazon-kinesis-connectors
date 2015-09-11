package com.amazonaws.services.kinesis.connectors.interfaces;

import java.io.IOException;
import java.util.Collection;

public interface IBatchingCollectionTransformer<T, U> extends ICollectionTransformer<T, U> {
    public Collection<U> fromClasses(Collection<T> record) throws IOException;
}
