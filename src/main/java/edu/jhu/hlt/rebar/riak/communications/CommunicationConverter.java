/**
 * 
 */
package edu.jhu.hlt.rebar.riak.communications;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.KeyUtil;
import com.basho.riak.client.convert.NoKeySpecifedException;
import com.basho.riak.client.http.util.Constants;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.jhu.hlt.rebar.riak.RiakCommunication;

/**
 * @author max
 *
 */
public class CommunicationConverter implements Converter<RiakCommunication> {

    private String bucket = "communications";
    
    /**
     * 
     */
    public CommunicationConverter() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public IRiakObject fromDomain(RiakCommunication domainObject, VClock vClock) throws ConversionException {
        String key = KeyUtil.getKey(domainObject);
        if (key == null)
            throw new NoKeySpecifedException(domainObject);
        
        byte[] commBytes = domainObject.getCommunicationBytes();
        return RiakObjectBuilder.newBuilder(bucket, key)
                .withValue(commBytes)
                .withVClock(vClock)
                .withContentType(Constants.CTYPE_OCTET_STREAM)
                .build();
    }

    @Override
    public RiakCommunication toDomain(IRiakObject riakObject) throws ConversionException {
        if (riakObject == null)
            return null;
        try {
            return new RiakCommunication(riakObject.getValue());
        } catch (InvalidProtocolBufferException e) {
            throw new ConversionException(e);
        }
    }

}
