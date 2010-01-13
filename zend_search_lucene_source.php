<?php
class ZendSearchLuceneSource extends DataSource {
    public $description = 'Zend_Search_Lucene index interface';
    public $indexFile = null;
    private $__index = null;

    function __construct($config) {
    	$this->indexFile = $config['indexFile'];
    	$this->__loadIndex(TMP.$this->indexFile);
        parent::__construct($config);
    }
    
	function read(&$model, $queryData = array()) {
		$items = $this->__readData($queryData['conditions']);
		
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

	function delete(&$model, $id = null) {
		if (!$id) {
			return false;
		}
		
		return $this->__delete($id);
	}
	
	function deleteAll() {
		return $this->__delete();
	}
	
	function save(&$model, $data) {
		if (!$data || empty($data) || !is_array($data) || count($data) < 1) {
			return false;
		}

		$doc = $this->__createSearchDocument();
		foreach ($data as $field) {
			$doc->addField(Zend_Search_Lucene_Field::$field['type']($field['key'], $field['value']));
		}
		
		$this->__index->addDocument($doc);
	}
	
	private function __createSearchDocument() {
		return new Zend_Search_Lucene_Document();
	}
	
	private function __createIndex($path) {
    	$this->__index = Zend_Search_Lucene::create($path);
    }
   
    private function __readData($queryData) {
		//$data = $this->__index->find($queryData['query']);
		$query = Zend_Search_Lucene_Search_QueryParser::parse($queryData['query']);
		$hits = $this->__index->find($query);
		
		$data = array();
		foreach ($hits as $i => $hit) {
			$data[$i] = array(
				'url' => $hit->url,
				'name' => $query->highlightMatches($hit->name),
				'description' => $query->highlightMatches($hit->description),
				'type' => $hit->type
			);
		}
		return $data;
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

	public function calculate(&$model, $func, $params = array()) {
		return '__'.$func;
	}	
	
	public function paginateCount() {
	
	}

}

?>