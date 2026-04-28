"""
图谱后端抽象层
支持 Zep Cloud 和 Graphiti(Neo4j) 两种后端。
"""

import asyncio
import json
import os
import threading
import time
import uuid
import warnings
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, get_origin

from pydantic import Field

from ..config import Config
from ..utils.locale import t
from ..utils.zep_paging import fetch_all_edges, fetch_all_nodes


class GraphBackend(ABC):
    """图谱后端统一接口。"""

    @abstractmethod
    def create_graph(self, name: str) -> str:
        pass

    @abstractmethod
    def set_ontology(self, graph_id: str, ontology: Dict[str, Any]):
        pass

    @abstractmethod
    def add_text_batches(
        self,
        graph_id: str,
        chunks: List[str],
        batch_size: int = 3,
        progress_callback: Optional[Callable] = None,
    ) -> List[str]:
        pass

    @abstractmethod
    def wait_for_episodes(
        self,
        episode_uuids: List[str],
        progress_callback: Optional[Callable] = None,
        timeout: int = 600,
    ):
        pass

    @abstractmethod
    def get_graph_data(self, graph_id: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def delete_graph(self, graph_id: str):
        pass

    @abstractmethod
    def search_graph(self, graph_id: str, query: str, limit: int = 10) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_all_nodes(self, graph_id: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_all_edges(self, graph_id: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_node_detail(self, node_uuid: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_node_edges(self, graph_id: str, node_uuid: str) -> List[Dict[str, Any]]:
        pass


class ZepBackend(GraphBackend):
    """Zep Cloud 后端实现（兼容现有逻辑）。"""

    def __init__(self, api_key: Optional[str] = None):
        from zep_cloud import EntityEdgeSourceTarget, EpisodeData
        from zep_cloud.client import Zep
        from zep_cloud.external_clients.ontology import EdgeModel, EntityModel, EntityText

        self._EpisodeData = EpisodeData
        self._EntityEdgeSourceTarget = EntityEdgeSourceTarget
        self._EntityModel = EntityModel
        self._EntityText = EntityText
        self._EdgeModel = EdgeModel

        self.api_key = api_key or Config.ZEP_API_KEY
        if not self.api_key:
            raise ValueError("ZEP_API_KEY 未配置")
        self.client = Zep(api_key=self.api_key)

    def create_graph(self, name: str) -> str:
        graph_id = f"mirofish_{uuid.uuid4().hex[:16]}"
        self.client.graph.create(
            graph_id=graph_id,
            name=name,
            description="MiroFish Social Simulation Graph",
        )
        return graph_id

    def set_ontology(self, graph_id: str, ontology: Dict[str, Any]):
        RESERVED_NAMES = {'uuid', 'name', 'group_id', 'name_embedding', 'summary', 'created_at'}

        def safe_attr_name(attr_name: str) -> str:
            if attr_name.lower() in RESERVED_NAMES:
                return f"entity_{attr_name}"
            return attr_name

        warnings.filterwarnings('ignore', category=UserWarning, module='pydantic')

        entity_types = {}
        for entity_def in ontology.get("entity_types", []):
            name = entity_def["name"]
            description = entity_def.get("description", f"A {name} entity.")
            attrs = {"__doc__": description}
            annotations = {}
            for attr_def in entity_def.get("attributes", []):
                attr_name = safe_attr_name(attr_def["name"])
                attr_desc = attr_def.get("description", attr_name)
                attrs[attr_name] = Field(description=attr_desc, default=None)
                annotations[attr_name] = Optional[self._EntityText]
            attrs["__annotations__"] = annotations
            entity_class = type(name, (self._EntityModel,), attrs)
            entity_class.__doc__ = description
            entity_types[name] = entity_class

        edge_definitions = {}
        for edge_def in ontology.get("edge_types", []):
            name = edge_def["name"]
            description = edge_def.get("description", f"A {name} relationship.")
            attrs = {"__doc__": description}
            annotations = {}
            for attr_def in edge_def.get("attributes", []):
                attr_name = safe_attr_name(attr_def["name"])
                attr_desc = attr_def.get("description", attr_name)
                attrs[attr_name] = Field(description=attr_desc, default=None)
                annotations[attr_name] = Optional[str]
            attrs["__annotations__"] = annotations
            class_name = ''.join(word.capitalize() for word in name.split('_'))
            edge_class = type(class_name, (self._EdgeModel,), attrs)
            edge_class.__doc__ = description
            source_targets = []
            for st in edge_def.get("source_targets", []):
                source_targets.append(
                    self._EntityEdgeSourceTarget(
                        source=st.get("source", "Entity"),
                        target=st.get("target", "Entity"),
                    )
                )
            if source_targets:
                edge_definitions[name] = (edge_class, source_targets)

        if entity_types or edge_definitions:
            self.client.graph.set_ontology(
                graph_ids=[graph_id],
                entities=entity_types if entity_types else None,
                edges=edge_definitions if edge_definitions else None,
            )

    def add_text_batches(
        self,
        graph_id: str,
        chunks: List[str],
        batch_size: int = 3,
        progress_callback: Optional[Callable] = None,
    ) -> List[str]:
        episode_uuids = []
        total_chunks = len(chunks)
        for i in range(0, total_chunks, batch_size):
            batch_chunks = chunks[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_chunks + batch_size - 1) // batch_size
            if progress_callback:
                progress = (i + len(batch_chunks)) / total_chunks
                progress_callback(
                    t('progress.sendingBatch', current=batch_num, total=total_batches, chunks=len(batch_chunks)),
                    progress,
                )
            episodes = [self._EpisodeData(data=chunk, type="text") for chunk in batch_chunks]
            batch_result = self.client.graph.add_batch(graph_id=graph_id, episodes=episodes)
            if batch_result and isinstance(batch_result, list):
                for ep in batch_result:
                    ep_uuid = getattr(ep, 'uuid_', None) or getattr(ep, 'uuid', None)
                    if ep_uuid:
                        episode_uuids.append(ep_uuid)
            time.sleep(1)
        return episode_uuids

    def wait_for_episodes(
        self,
        episode_uuids: List[str],
        progress_callback: Optional[Callable] = None,
        timeout: int = 600,
    ):
        if not episode_uuids:
            if progress_callback:
                progress_callback(t('progress.noEpisodesWait'), 1.0)
            return

        start_time = time.time()
        pending_episodes = set(episode_uuids)
        completed_count = 0
        total_episodes = len(episode_uuids)

        while pending_episodes:
            if time.time() - start_time > timeout:
                break
            for ep_uuid in list(pending_episodes):
                try:
                    episode = self.client.graph.episode.get(uuid_=ep_uuid)
                    if getattr(episode, 'processed', False):
                        pending_episodes.remove(ep_uuid)
                        completed_count += 1
                except Exception:
                    pass
            if progress_callback:
                progress_callback(
                    t('progress.zepProcessing', completed=completed_count, total=total_episodes, pending=len(pending_episodes), elapsed=int(time.time() - start_time)),
                    completed_count / total_episodes if total_episodes > 0 else 0,
                )
            if pending_episodes:
                time.sleep(3)
        if progress_callback:
            progress_callback(t('progress.processingComplete', completed=completed_count, total=total_episodes), 1.0)

    def get_all_nodes(self, graph_id: str) -> List[Dict[str, Any]]:
        nodes = fetch_all_nodes(self.client, graph_id)
        result = []
        for node in nodes:
            result.append({
                "uuid": getattr(node, "uuid_", None) or getattr(node, "uuid", ""),
                "name": getattr(node, "name", ""),
                "labels": getattr(node, "labels", []) or [],
                "summary": getattr(node, "summary", "") or "",
                "attributes": getattr(node, "attributes", {}) or {},
                "created_at": str(getattr(node, "created_at", None)) if getattr(node, "created_at", None) else None,
            })
        return result

    def get_all_edges(self, graph_id: str) -> List[Dict[str, Any]]:
        edges = fetch_all_edges(self.client, graph_id)
        result = []
        for edge in edges:
            result.append({
                "uuid": getattr(edge, "uuid_", None) or getattr(edge, "uuid", ""),
                "name": getattr(edge, "name", "") or "",
                "fact": getattr(edge, "fact", "") or "",
                "source_node_uuid": getattr(edge, "source_node_uuid", ""),
                "target_node_uuid": getattr(edge, "target_node_uuid", ""),
                "created_at": str(getattr(edge, "created_at", None)) if getattr(edge, "created_at", None) else None,
                "valid_at": str(getattr(edge, "valid_at", None)) if getattr(edge, "valid_at", None) else None,
                "invalid_at": str(getattr(edge, "invalid_at", None)) if getattr(edge, "invalid_at", None) else None,
                "expired_at": str(getattr(edge, "expired_at", None)) if getattr(edge, "expired_at", None) else None,
                "episodes": [str(e) for e in (getattr(edge, "episodes", None) or getattr(edge, "episode_ids", None) or [])],
            })
        return result

    def get_graph_data(self, graph_id: str) -> Dict[str, Any]:
        nodes_data = self.get_all_nodes(graph_id)
        edges_data = self.get_all_edges(graph_id)
        node_map = {n["uuid"]: n["name"] for n in nodes_data}
        for edge in edges_data:
            edge["source_node_name"] = node_map.get(edge["source_node_uuid"], "")
            edge["target_node_name"] = node_map.get(edge["target_node_uuid"], "")
            edge["fact_type"] = edge.get("name", "")
            edge["attributes"] = edge.get("attributes", {})
        return {
            "graph_id": graph_id,
            "nodes": nodes_data,
            "edges": edges_data,
            "node_count": len(nodes_data),
            "edge_count": len(edges_data),
        }

    def delete_graph(self, graph_id: str):
        self.client.graph.delete(graph_id=graph_id)

    def search_graph(self, graph_id: str, query: str, limit: int = 10) -> Dict[str, Any]:
        search_results = self.client.graph.search(
            graph_id=graph_id,
            query=query,
            limit=limit,
            scope="edges",
            reranker="cross_encoder",
        )
        facts = []
        edges = []
        nodes = []
        if hasattr(search_results, 'edges') and search_results.edges:
            for edge in search_results.edges:
                if hasattr(edge, 'fact') and edge.fact:
                    facts.append(edge.fact)
                edges.append({
                    "uuid": getattr(edge, 'uuid_', None) or getattr(edge, 'uuid', ''),
                    "name": getattr(edge, 'name', ''),
                    "fact": getattr(edge, 'fact', ''),
                    "source_node_uuid": getattr(edge, 'source_node_uuid', ''),
                    "target_node_uuid": getattr(edge, 'target_node_uuid', ''),
                })
        if hasattr(search_results, 'nodes') and search_results.nodes:
            for node in search_results.nodes:
                nodes.append({
                    "uuid": getattr(node, 'uuid_', None) or getattr(node, 'uuid', ''),
                    "name": getattr(node, 'name', ''),
                    "labels": getattr(node, 'labels', []),
                    "summary": getattr(node, 'summary', ''),
                })
        return {"facts": facts, "edges": edges, "nodes": nodes}

    def get_node_detail(self, node_uuid: str) -> Optional[Dict[str, Any]]:
        node = self.client.graph.node.get(uuid_=node_uuid)
        if not node:
            return None
        return {
            "uuid": getattr(node, "uuid_", None) or getattr(node, "uuid", ""),
            "name": node.name or "",
            "labels": node.labels or [],
            "summary": node.summary or "",
            "attributes": node.attributes or {},
        }

    def get_node_edges(self, graph_id: str, node_uuid: str) -> List[Dict[str, Any]]:
        return [
            edge for edge in self.get_all_edges(graph_id)
            if edge.get("source_node_uuid") == node_uuid or edge.get("target_node_uuid") == node_uuid
        ]


class GraphitiBackend(GraphBackend):
    """Graphiti + Neo4j 后端实现。"""

    def __init__(self):
        from graphiti_core import Graphiti
        from graphiti_core.embedder import OpenAIEmbedder, OpenAIEmbedderConfig
        from graphiti_core.edges import EntityEdge
        from graphiti_core.llm_client import LLMConfig
        from graphiti_core.llm_client.client import LLMClient
        from graphiti_core.llm_client.config import DEFAULT_MAX_TOKENS, ModelSize
        from graphiti_core.nodes import EntityNode, EpisodeType, Node
        from openai import AsyncOpenAI
        from pydantic import BaseModel

        class JsonModeOpenAIClient(LLMClient):
            """OpenAI-compatible client using JSON mode instead of schema parse mode."""

            def __init__(self, config: LLMConfig):
                super().__init__(config, cache=False)
                self.client = AsyncOpenAI(api_key=config.api_key, base_url=config.base_url)

            async def _generate_response(
                self,
                messages: list,
                response_model: type[BaseModel] | None = None,
                max_tokens: int = DEFAULT_MAX_TOKENS,
                model_size: ModelSize = ModelSize.medium,
            ) -> dict[str, Any]:
                model = self.small_model if model_size == ModelSize.small else self.model
                openai_messages = [
                    {"role": m.role, "content": self._clean_input(m.content)}
                    for m in messages
                    if m.role in {"system", "user", "assistant"}
                ]
                response = await self.client.chat.completions.create(
                    model=model,
                    messages=openai_messages,
                    temperature=self.temperature,
                    max_tokens=max_tokens or self.max_tokens,
                    response_format={"type": "json_object"},
                )
                content = (response.choices[0].message.content or "").strip()
                if not content:
                    raise ValueError("Empty response content from LLM")
                data = json.loads(content)
                if response_model is not None:
                    try:
                        validated = response_model.model_validate(data)
                        return validated.model_dump()
                    except Exception:
                        # Some providers occasionally omit required primitive fields
                        # (e.g. "summary"). Try a conservative repair for missing keys.
                        if isinstance(data, dict):
                            repaired_data = dict(data)
                            for field_name, field_info in response_model.model_fields.items():
                                if field_name in repaired_data or not field_info.is_required():
                                    continue
                                annotation = field_info.annotation
                                origin = get_origin(annotation)
                                if annotation is str:
                                    repaired_data[field_name] = ""
                                elif annotation is int:
                                    repaired_data[field_name] = 0
                                elif annotation is float:
                                    repaired_data[field_name] = 0.0
                                elif annotation is bool:
                                    repaired_data[field_name] = False
                                elif origin in (list, List):
                                    repaired_data[field_name] = []
                                elif origin in (dict, Dict):
                                    repaired_data[field_name] = {}
                                else:
                                    repaired_data[field_name] = None
                            validated = response_model.model_validate(repaired_data)
                            return validated.model_dump()
                        raise
                return data

        # 使用现有 LLM 配置驱动 Graphiti 默认 OpenAI 客户端
        if Config.LLM_API_KEY and not os.environ.get("OPENAI_API_KEY"):
            os.environ["OPENAI_API_KEY"] = Config.LLM_API_KEY
        if Config.LLM_BASE_URL and not os.environ.get("OPENAI_BASE_URL"):
            os.environ["OPENAI_BASE_URL"] = Config.LLM_BASE_URL

        self._Graphiti = Graphiti
        self._OpenAIEmbedder = OpenAIEmbedder
        self._OpenAIEmbedderConfig = OpenAIEmbedderConfig
        self._OpenAIClient = JsonModeOpenAIClient
        self._LLMConfig = LLMConfig
        self._provider_profiles = self._build_provider_profiles()
        self._graphiti_clients = [self._create_graphiti_client(profile) for profile in self._provider_profiles]
        self._active_provider_idx = 0
        self.graphiti = self._graphiti_clients[self._active_provider_idx]
        self._EntityNode = EntityNode
        self._EntityEdge = EntityEdge
        self._Node = Node
        self._EpisodeType = EpisodeType
        self._indices_initialized = False
        # Graphiti/Neo4j async driver objects are loop-bound.
        # Keep one dedicated loop for this backend instance.
        self._loop = asyncio.new_event_loop()
        self._loop_lock = threading.Lock()

    def _run_async(self, coro):
        with self._loop_lock:
            return self._loop.run_until_complete(coro)

    def _build_provider_profiles(self) -> List[Dict[str, str]]:
        profiles: List[Dict[str, str]] = []
        profiles.append({
            "name": "primary",
            "api_key": Config.LLM_API_KEY,
            "base_url": Config.LLM_BASE_URL,
            "llm_model": Config.GRAPHITI_LLM_MODEL,
            "embedding_model": Config.GRAPHITI_EMBEDDING_MODEL,
        })
        if all([Config.LLM_BOOST_API_KEY, Config.LLM_BOOST_BASE_URL, Config.LLM_BOOST_MODEL_NAME]):
            profiles.append({
                "name": "boost",
                "api_key": Config.LLM_BOOST_API_KEY,
                "base_url": Config.LLM_BOOST_BASE_URL,
                "llm_model": Config.LLM_BOOST_MODEL_NAME,
                "embedding_model": Config.LLM_BOOST_EMBEDDING_MODEL or Config.GRAPHITI_EMBEDDING_MODEL,
            })
        if all([Config.GRAPHITI_RETRY3_API_KEY, Config.GRAPHITI_RETRY3_BASE_URL, Config.GRAPHITI_RETRY3_LLM_MODEL, Config.GRAPHITI_RETRY3_EMBEDDING_MODEL]):
            profiles.append({
                "name": "retry3",
                "api_key": Config.GRAPHITI_RETRY3_API_KEY,
                "base_url": Config.GRAPHITI_RETRY3_BASE_URL,
                "llm_model": Config.GRAPHITI_RETRY3_LLM_MODEL,
                "embedding_model": Config.GRAPHITI_RETRY3_EMBEDDING_MODEL,
            })
        return profiles[:3]

    def _create_graphiti_client(self, profile: Dict[str, str]):
        llm_config = self._LLMConfig(
            api_key=profile["api_key"],
            base_url=profile["base_url"],
            model=profile["llm_model"],
            small_model=profile["llm_model"],
        )
        embedder_config = self._OpenAIEmbedderConfig(
            api_key=profile["api_key"],
            base_url=profile["base_url"],
            embedding_model=profile["embedding_model"],
        )
        return self._Graphiti(
            uri=Config.NEO4J_URI,
            user=Config.NEO4J_USER,
            password=Config.NEO4J_PASSWORD,
            llm_client=self._OpenAIClient(config=llm_config),
            embedder=self._OpenAIEmbedder(config=embedder_config),
        )

    def _ensure_indices(self):
        if not self._indices_initialized:
            self._run_async(self.graphiti.build_indices_and_constraints(delete_existing=False))
            self._indices_initialized = True

    def create_graph(self, name: str) -> str:
        self._ensure_indices()
        return f"mirofish_{uuid.uuid4().hex[:16]}"

    def set_ontology(self, graph_id: str, ontology: Dict[str, Any]):
        # Graphiti 对本体是可选增强，MVP阶段保持接口兼容，不阻塞构建流程。
        return

    def add_text_batches(
        self,
        graph_id: str,
        chunks: List[str],
        batch_size: int = 3,
        progress_callback: Optional[Callable] = None,
    ) -> List[str]:
        self._ensure_indices()
        episode_uuids: List[str] = []
        total_chunks = len(chunks)
        for i, chunk in enumerate(chunks):
            if progress_callback:
                progress_callback(
                    t('progress.sendingBatch', current=i + 1, total=total_chunks, chunks=1),
                    (i + 1) / total_chunks if total_chunks else 1.0,
                )

            last_error = None
            for provider_idx, provider in enumerate(self._provider_profiles):
                self._active_provider_idx = provider_idx
                self.graphiti = self._graphiti_clients[provider_idx]
                try:
                    result = self._run_async(
                        self.graphiti.add_episode(
                            name=f"episode_{i + 1}",
                            episode_body=chunk,
                            source_description="MiroFish text chunk",
                            reference_time=datetime.utcnow(),
                            source=self._EpisodeType.text,
                            group_id=graph_id,
                        )
                    )
                    episode_uuids.append(result.episode.uuid)
                    last_error = None
                    break
                except Exception as e:
                    last_error = e
                    if progress_callback:
                        progress_callback(
                            f"Chunk {i + 1}/{total_chunks} failed on provider '{provider['name']}', trying next provider...",
                            (i + 1) / total_chunks if total_chunks else 1.0,
                        )
            if last_error is not None:
                raise RuntimeError(
                    f"All provider retries failed for chunk {i + 1}/{total_chunks}: {last_error}"
                )
        return episode_uuids

    def wait_for_episodes(
        self,
        episode_uuids: List[str],
        progress_callback: Optional[Callable] = None,
        timeout: int = 600,
    ):
        if progress_callback:
            progress_callback(t('progress.processingComplete', completed=len(episode_uuids), total=len(episode_uuids)), 1.0)

    def get_all_nodes(self, graph_id: str) -> List[Dict[str, Any]]:
        nodes = self._run_async(self._EntityNode.get_by_group_ids(self.graphiti.driver, [graph_id]))
        return [
            {
                "uuid": n.uuid,
                "name": n.name,
                "labels": n.labels or [],
                "summary": n.summary or "",
                "attributes": n.attributes or {},
                "created_at": str(n.created_at) if n.created_at else None,
            }
            for n in nodes
        ]

    def get_all_edges(self, graph_id: str) -> List[Dict[str, Any]]:
        try:
            edges = self._run_async(self._EntityEdge.get_by_group_ids(self.graphiti.driver, [graph_id]))
        except Exception:
            edges = []
        return [
            {
                "uuid": e.uuid,
                "name": e.name or "",
                "fact": e.fact or "",
                "source_node_uuid": e.source_node_uuid,
                "target_node_uuid": e.target_node_uuid,
                "created_at": str(e.created_at) if e.created_at else None,
                "valid_at": str(e.valid_at) if e.valid_at else None,
                "invalid_at": str(e.invalid_at) if e.invalid_at else None,
                "expired_at": str(e.expired_at) if e.expired_at else None,
                "episodes": [str(ep) for ep in (e.episodes or [])],
            }
            for e in edges
        ]

    def get_graph_data(self, graph_id: str) -> Dict[str, Any]:
        nodes_data = self.get_all_nodes(graph_id)
        edges_data = self.get_all_edges(graph_id)
        node_map = {n["uuid"]: n["name"] for n in nodes_data}
        for edge in edges_data:
            edge["source_node_name"] = node_map.get(edge["source_node_uuid"], "")
            edge["target_node_name"] = node_map.get(edge["target_node_uuid"], "")
            edge["fact_type"] = edge.get("name", "")
            edge["attributes"] = {}
        return {
            "graph_id": graph_id,
            "nodes": nodes_data,
            "edges": edges_data,
            "node_count": len(nodes_data),
            "edge_count": len(edges_data),
        }

    def delete_graph(self, graph_id: str):
        self._run_async(self._Node.delete_by_group_id(self.graphiti.driver, graph_id))

    def search_graph(self, graph_id: str, query: str, limit: int = 10) -> Dict[str, Any]:
        edges = self._run_async(self.graphiti.search(query=query, group_ids=[graph_id], num_results=limit))
        facts = [edge.fact for edge in edges if getattr(edge, "fact", None)]
        return {
            "facts": facts,
            "edges": [
                {
                    "uuid": edge.uuid,
                    "name": edge.name,
                    "fact": edge.fact,
                    "source_node_uuid": edge.source_node_uuid,
                    "target_node_uuid": edge.target_node_uuid,
                }
                for edge in edges
            ],
            "nodes": [],
        }

    def get_node_detail(self, node_uuid: str) -> Optional[Dict[str, Any]]:
        try:
            node = self._run_async(self._EntityNode.get_by_uuid(self.graphiti.driver, node_uuid))
        except Exception:
            return None
        return {
            "uuid": node.uuid,
            "name": node.name,
            "labels": node.labels or [],
            "summary": node.summary or "",
            "attributes": node.attributes or {},
        }

    def get_node_edges(self, graph_id: str, node_uuid: str) -> List[Dict[str, Any]]:
        edges = self._run_async(self._EntityEdge.get_by_node_uuid(self.graphiti.driver, node_uuid))
        return [
            {
                "uuid": e.uuid,
                "name": e.name or "",
                "fact": e.fact or "",
                "source_node_uuid": e.source_node_uuid,
                "target_node_uuid": e.target_node_uuid,
                "created_at": str(e.created_at) if e.created_at else None,
                "valid_at": str(e.valid_at) if e.valid_at else None,
                "invalid_at": str(e.invalid_at) if e.invalid_at else None,
                "expired_at": str(e.expired_at) if e.expired_at else None,
                "episodes": [str(ep) for ep in (e.episodes or [])],
            }
            for e in edges
            if e.group_id == graph_id
        ]

    def __del__(self):
        try:
            with self._loop_lock:
                if not self._loop.is_closed():
                    for graphiti_client in self._graphiti_clients:
                        try:
                            self._loop.run_until_complete(graphiti_client.close())
                        except Exception:
                            pass
                    self._loop.close()
        except Exception:
            pass


def get_graph_backend() -> GraphBackend:
    backend = Config.GRAPH_BACKEND
    if backend == 'graphiti':
        return GraphitiBackend()
    return ZepBackend(api_key=Config.ZEP_API_KEY)

