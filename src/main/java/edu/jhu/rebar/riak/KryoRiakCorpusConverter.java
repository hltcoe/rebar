/**
 * 
 */
package edu.jhu.rebar.riak;

import java.util.TreeSet;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.convert.Converter;
import com.esotericsoftware.kryo.Kryo;


/**
 * Implementation of {@link Converter} interface that uses Kryo library to serialize
 * {@link RiakCorpus} objects to and from Riak. 
 * 
 * @author max
 * 
 */
public class KryoRiakCorpusConverter implements Converter<RiakCorpus> {

    private String bucket;
    private Kryo kryo;


    /**
     * Ctor where one can pass in a bucket name to the converter.
     */
    public KryoRiakCorpusConverter(String bucket) {
        this.bucket = bucket;
        this.kryo = new Kryo();
        this.kryo.register(RiakCorpus.class);
        this.kryo.register(TreeSet.class);
    }
    
    /**
     * No arg ctor - default to "corpora" bucket
     */
    public KryoRiakCorpusConverter() {
        this(RiakConstants.CORPORA_BUCKET_NAME);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.convert.Converter#fromDomain(java.lang.Object,
     * com.basho.riak.client.cap.VClock)
     */
    @Override
    public IRiakObject fromDomain(RiakCorpus domainObject, VClock vClock) throws ConversionException {
//        String key = KeyUtil.getKey(domainObject);
//
//        if (key == null) {
//            throw new NoKeySpecifedException(domainObject);
//        }
//
//        ObjectBuffer ob = new ObjectBuffer(kryo, 2 * 1024, 25 * 1024 * 1024);
//        byte[] value = ob.writeObject(domainObject);
//
//        return RiakObjectBuilder.newBuilder(bucket, key)
//                .withValue(value)
//                .withVClock(vClock)
//                .withContentType(Constants.CTYPE_OCTET_STREAM)
//                .build();
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.convert.Converter#toDomain(com.basho.riak.client
     * .IRiakObject)
     */
    @Override
    public RiakCorpus toDomain(IRiakObject riakObject) throws ConversionException {
//        if (riakObject == null)
//            return null;
//        ObjectBuffer ob = new ObjectBuffer(this.kryo);
//        return ob.readObject(riakObject.getValue(), RiakCorpus.class);
        return null;
    }

}
