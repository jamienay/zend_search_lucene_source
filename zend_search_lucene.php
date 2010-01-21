<?php
class ZendSearchLucene extends DataSource {
    public $description = 'Zend_Search_Lucene index interface';
    
    public $indexFile = null;
    
    protected $_schema = array(
    	'document' => array()
    );
    
    protected $sources = array('search_indices');
    
    private $__index = null;
    

    public function __construct($config) {
    	$this->indexFile = $config['indexFile'];
    	$this->__setSources($config['source']);
    	$this->__loadIndex(TMP.$this->indexFile);
        parent::__construct($config);
    }

	public function read(&$model, $queryData = array()) {
		$items = $this->__readData(&$model, $queryData);
		if ($items) {
			$items = $this->__getPage($items, $queryData);
			
			// A request for a count (from paginate or otherwise).
			if ( Set::extract($queryData, 'fields') == '__count' ) {
				return array(array($model->alias => array('count' => count($items))));
			}
		} else {
			// A request for a count (from paginate or otherwise).
			if (Set::extract($queryData, 'fields') == '__count') {
				return array(array($model->alias => array('count' => count($items))));
			}
		}
	
		return $items;
	}

	public function delete(&$model, $id = null) {
		if (!$id) {
			return $this->__delete();
		}
		
		return $this->__delete($id);
	}
	
	public function deleteAll() {
		return $this->__delete();
	}

	public function create(&$model, $fields = array(), $values = array()) {
		$count = $this->__index->count();
		
		foreach ($fields as $i => $field) {
			$doc = $this->__createSearchDocument();
			foreach ($values[$i] as $key => $val) {
				$doc->addField(Zend_Search_Lucene_Field::$val['type']($val['key'], strip_tags($val['value']), 'utf-8'));
			}
			
			
			$this->__index->addDocument($doc);
		}
		
		if ($this->__index->count() > $count) {
			return true;
		}
		
		return false;
	}
	
	public function calculate(&$model, $func, $params = array()) {
		return '__'.$func;
	}	
	
	public function paginateCount() {
	
	}
	
	/**
	 * This is just here, empty log array and all, for DebugKit compatibility.
	 */
	public function getLog() {
		return array('log' => array());
	}
	
	public function describe(&$model) {
		return $this->_schema;
	}
	
	public function listSources() {
		return $this->sources;
	}
	
	private function __createSearchDocument() {
		return new Zend_Search_Lucene_Document();
	}
	
	private function __createIndex($path) {
    	$this->__index = Zend_Search_Lucene::create($path);
    }
   
    private function __readData(&$model, $queryData) {
    	$highlight = false;
    	if (isset($queryData['highlight']) && $queryData['highlight'] == true) {
    		$highlight = true;
    	}
    	$query = $this->__parseQuery($queryData);
		$hits = $this->__index->find($query);

		$data = array();
		foreach ($hits as $i => $hit) {
			$fields = $this->__getFieldInfo($hit->getDocument());
			
			$returnArray = array();
			foreach ($fields as $field) {
				if ($highlight && $field->isIndexed == 1) {
					$returnArray[$field->name] = $query->htmlFragmentHighlightMatches($hit->{$field->name});
				} else {
					$returnArray[$field->name] = $hit->{$field->name};
				}
			}

			$data[$i][$model->alias] = $returnArray;
		}
		return $data;
	}
	
	private function __parseQuery($queryData) {
		$queryString = $queryData['conditions']['query'];
    	return Zend_Search_Lucene_Search_QueryParser::parse($queryString);
	}
	
	private function __getFieldInfo(Zend_Search_Lucene_Document $doc) {
		$fieldNames = $doc->getFieldNames();
		$fields = array();
		foreach ($fieldNames as $fieldName) {
			$fields[] = $doc->getField($fieldName);
		}
		
		return $fields;
	}
	
	private function __delete($index = null) {
		if (!$index) {
			return $this->__createIndex(TMP.$this->indexFile);
		} else {
			return $this->__index->delete($index);
		}
		
		return false;
	}
	
	private function __loadIndex($path) {
    	if (file_exists($path)) {
    		return $this->__index = Zend_Search_Lucene::open($path);
    	} else {
    		return $this->__createIndex($path);
    	}
    	
    	return false;
    }
    
    private function __getPage($items = null, $queryData = array()) {
		if (empty($queryData['limit']))  {
			return $items;
		}
		$limit = $queryData['limit'];
		$page = $queryData['page'];

		$offset = $limit * ($page-1);
		return array_slice($items, $offset, $limit);
	}
	
	private function __setSources($configSource) {
    	if (!$configSource) {
    		return $this->sources;
    	}
    	
    	if (!is_array($configSource)) {
    		$this->sources = array($configSource);
    		return $this->sources;
    	}
    	
    	$this->sources = $configSource;
    	return $this->sources;
    }

}

?>