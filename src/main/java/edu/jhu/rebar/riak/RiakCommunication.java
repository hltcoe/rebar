/**
 * 
 */
package edu.jhu.rebar.riak;

import com.basho.riak.client.convert.RiakKey;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.jhu.concrete.Concrete.Communication;
import edu.jhu.rebar.util.IdUtil;

/**
 * Wrapper around {@link Communication} object that provides Riak functionality.
 * 
 * @author max
 *
 */
public class RiakCommunication {

    @RiakKey
    private String id;
    private Communication comm;
    
    /**
     * 
     */
    public RiakCommunication(Communication comm) {
        this.comm = comm;
        this.id = IdUtil.uuidToString(comm.getUuid());
    }
    
    public RiakCommunication(byte[] commBytes) throws InvalidProtocolBufferException {
        this(Communication.parseFrom(commBytes));
    }
    
    public RiakCommunication (String id) {
        this.id = id;
    }

    /**
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * @return the comm
     */
    public Communication getComm() {
        return comm;
    }

    /**
     * @param comm the comm to set
     */
    public void setComm(Communication comm) {
        this.comm = comm;
    }
    
    public byte[] getCommunicationBytes() {
        return this.comm.toByteArray();
    }
    

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("RiakCommunication [id=");
        builder.append(id);
        builder.append(", text=");
        if (comm != null)
            builder.append(comm.getText());
        else
            builder.append("{no comm set}");
        builder.append("]");
        return builder.toString();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RiakCommunication other = (RiakCommunication) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }
}
