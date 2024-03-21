package org.springframework.data.redis.connection.jedis;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.FallbackExceptionTranslationStrategy;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.AbstractRedisConnection;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.Response;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Jingzhang Chen
 * @since 3.3.1
 */
public abstract class AbstractPipelineRedisConnection<C extends JedisCommands> extends AbstractRedisConnection {

    private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION =
            new FallbackExceptionTranslationStrategy(JedisExceptionConverter.INSTANCE);

    private final JedisInvoker<C> invoker = new JedisInvoker<>((directFunction, pipelineFunction, converter,
                                                                           nullDefault) -> doInvoke(false, directFunction, pipelineFunction, converter, nullDefault));

    private final JedisInvoker<C> statusInvoker = new JedisInvoker<>((directFunction, pipelineFunction, converter,
                                                                                 nullDefault) -> doInvoke(true, directFunction, pipelineFunction, converter, nullDefault));

    @SuppressWarnings("rawtypes")
    private List<JedisResult> pipelinedResults = new ArrayList<>();

    private volatile @Nullable PipelineBase pipeline;

    @Nullable
    private Object doInvoke(boolean status, Function<C, Object> directFunction,
                            Function<JedisInvoker.ResponseCommands, Response<Object>> pipelineFunction, Converter<Object, Object> converter,
                            Supplier<Object> nullDefault) {

        return doWithClient(it -> {

            // if (isQueueing()) {
            //
            //     Response<Object> response = pipelineFunction.apply(JedisInvoker.createCommands(getRequiredTransaction()));
            //     transaction(status ? newStatusResult(response) : newJedisResult(response, converter, nullDefault));
            //     return null;
            // }

            if (isPipelined()) {

                Response<Object> response = pipelineFunction.apply(JedisInvoker.createCommands(getRequiredPipeline()));
                pipeline(status ? newStatusResult(response) : newJedisResult(response, converter, nullDefault));
                return null;
            }

            Object result = directFunction.apply(getClient());

            if (result == null) {
                return nullDefault.get();
            }

            return converter.convert(result);
        });
    }

    abstract C getClient();

    <T> JedisResult<T, T> newJedisResult(Response<T> response) {
        return JedisResult.JedisResultBuilder.<T, T> forResponse(response).build();
    }

    <T, R> JedisResult<T, R> newJedisResult(Response<T> response, Converter<T, R> converter, Supplier<R> defaultValue) {

        return JedisResult.JedisResultBuilder.<T, R> forResponse(response).mappedWith(converter)
                // TODO fixme
                .convertPipelineAndTxResults(false).mapNullTo(defaultValue).build();
    }


    <T> JedisResult.JedisStatusResult<T, T> newStatusResult(Response<T> response) {
        return JedisResult.JedisResultBuilder.<T, T> forResponse(response).buildStatusResult();
    }


    @Override
    public boolean isPipelined() {
        return this.pipeline != null;
    }
    @Override
    public void openPipeline() {

        if (isQueueing()) {
            throw new InvalidDataAccessApiUsageException("Cannot use Pipelining while a transaction is active");
        }

        if (pipeline == null) {
            pipeline = newPipeline();
        }
    }

    abstract PipelineBase newPipeline();

    @Override
    public List<Object> closePipeline() {

        if (pipeline != null) {
            try {
                return convertPipelineResults();
            } finally {
                pipeline = null;
                pipelinedResults.clear();
            }
        }

        return Collections.emptyList();
    }

    private List<Object> convertPipelineResults() {

        List<Object> results = new ArrayList<>();

        getRequiredPipeline().sync();

        Exception cause = null;

        for (JedisResult<?, ?> result : pipelinedResults) {
            try {

                Object data = result.get();

                if (!result.isStatus()) {
                    results.add(result.conversionRequired() ? result.convert(data) : data);
                }
            } catch (JedisDataException ex) {
                DataAccessException dataAccessException = convertJedisAccessException(ex);
                if (cause == null) {
                    cause = dataAccessException;
                }
                results.add(dataAccessException);
            } catch (DataAccessException ex) {
                if (cause == null) {
                    cause = ex;
                }
                results.add(ex);
            }
        }

        if (cause != null) {
            throw new RedisPipelineException(cause, results);
        }

        return results;
    }

    @Nullable
    public PipelineBase getPipeline() {
        return this.pipeline;
    }

    public PipelineBase getRequiredPipeline() {

        PipelineBase pipeline = getPipeline();

        Assert.state(pipeline != null, "Connection has no active pipeline");

        return pipeline;
    }

    void pipeline(JedisResult<?, ?> result) {
        pipelinedResults.add(result);
    }

    protected DataAccessException convertJedisAccessException(Exception cause) {
        DataAccessException exception = EXCEPTION_TRANSLATION.translate(cause);
        return exception != null ? exception : new RedisSystemException(cause.getMessage(), cause);
    }

    /**
     * Obtain a {@link JedisInvoker} to call Jedis methods on the current {@link Jedis} instance.
     *
     * @return the {@link JedisInvoker}.
     * @since 2.5
     */
    JedisInvoker<C> invoke() {
        return this.invoker;
    }

    /**
     * Obtain a {@link JedisInvoker} to call Jedis methods returning a status response on the current {@link Jedis}
     * instance. Status responses are not included in transactional and pipeline results.
     *
     * @return the {@link JedisInvoker}.
     * @since 2.5
     */
    JedisInvoker<C> invokeStatus() {
        return this.statusInvoker;
    }

     protected void doWithClient(Consumer<C> callback) {

        try {
            callback.accept(getClient());
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }
    @Nullable
    protected  <T> T doWithClient(Function<C, T> callback) {

        try {
            return callback.apply(getClient());
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }
}
