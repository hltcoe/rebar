///**
// * 
// */
//package edu.jhu.hlt.rebar.services;
//
//import org.apache.accumulo.core.client.Connector;
//
//import edu.jhu.hlt.concrete.Reader;
//import edu.jhu.hlt.rebar.Constants;
//import edu.jhu.hlt.rebar.RebarException;
//import edu.jhu.hlt.rebar.accumulo.RebarReader;
//
///**
// * @author max
// *
// */
//public class RebarReaderServer extends AbstractThriftServer {
//  
//  /**
//   * @param port
//   * @throws RebarException
//   */
//  public RebarReaderServer(int port) throws RebarException {
//    this(Constants.getConnector(), port);
//  }
//  
//  public RebarReaderServer(Connector conn, int port) throws RebarException {
//    this(conn, port, new RebarReader(conn));
//  }
//  
//  public RebarReaderServer(Connector conn, int port, RebarReader rr) throws RebarException {
//    super(port, rr, new Reader.Processor<>(rr));
//  }
//}
