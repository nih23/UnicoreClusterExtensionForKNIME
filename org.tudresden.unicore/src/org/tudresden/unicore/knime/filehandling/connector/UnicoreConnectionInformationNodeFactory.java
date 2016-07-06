package org.tudresden.unicore.knime.filehandling.connector;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;
import org.tudresden.unicore.knime.filehandling.UnicoreRemoteFileHandler;

/**
 * factory class for unicore connection objects
 * 
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 */
public class UnicoreConnectionInformationNodeFactory extends NodeFactory<UnicoreConnectionInformationNodeModel> {

	
	    /**
	     * {@inheritDoc}
	     */
	    @Override
	    public UnicoreConnectionInformationNodeModel createNodeModel() {
	        return new UnicoreConnectionInformationNodeModel(UnicoreRemoteFileHandler.PROTOCOL);
	    }

	    /**
	     * {@inheritDoc}
	     */
	    @Override
	    public int getNrNodeViews() {
	        return 0;
	    }

	    /**
	     * {@inheritDoc}
	     */
	    @Override
	    public NodeView<UnicoreConnectionInformationNodeModel> createNodeView(final int viewIndex,
	            final UnicoreConnectionInformationNodeModel nodeModel) {
	        return null;
	    }

	    /**
	     * {@inheritDoc}
	     */
	    @Override
	    public boolean hasDialog() {
	        return true;
	    }

	    /**
	     * {@inheritDoc}
	     */
	    @Override
	    public NodeDialogPane createNodeDialogPane() {
	    	return new UnicoreConnectionInformationNodePropertyPane();
	    }
	    
	    

	}
