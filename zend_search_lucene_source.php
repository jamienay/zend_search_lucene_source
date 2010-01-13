<?php
/**
* Zend_Search_Lucene Datasource for CakePHP 1.2/1.3
*
* A datasource for the Zend_Search_Lucene search index.
*
* @filesource
* @author Jamie Nay
* @copyright Jamie Nay
* @license http://www.opensource.org/licenses/mit-license.php The MIT License
* @link http://jamienay.com/2010/01/zend_search_lucene-datasource-for-cakephp
*/
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
   
    private function __readData(&$model, $queryData) {
    	$highlight = false;
    	if (isset($queryData['highlight']) && $queryData['highlight'] == true) {
    		$highlight = true;
    	}
    	
		$query = Zend_Search_Lucene_Search_QueryParser::parse($queryData['conditions']['query']);
		$hits = $this->__index->find($query);
		
		$data = array();
		foreach ($hits as $i => $hit) {
			$fields = $this->__getFieldInfo($hit->getDocument());
			
			$returnArray = array();
			foreach ($fields as $field) {
				if ($highlight && $field->isIndexed == 1) {
					$returnArray[$field->name] = $query->highlightMatches($hit->{$field->name});
				} else {
					$returnArray[$field->name] = $hit->{$field->name};
				}
			}

			$data[$i][$model->alias] = $returnArray;
		}
		return $data;
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

	public function calculate(&$model, $func, $params = array()) {
		return '__'.$func;
	}	
	
	public function paginateCount() {
	
	}

}
?>