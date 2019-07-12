package com.gabry.job.core.registry

import com.gabry.job.core.domain.Node
import com.gabry.job.core.registry.RegistryEvent.RegistryEvent

/**
  * Created by gabry on 2018/4/23 13:30
  * 注册中心通知接口
  */
/**
  * this:Registry 要求RegistryNotify在实例化时或定义RegistryNotify的子类时，必须混入指定的Registry类型，这个Registry类型也可以指定为当前类型
  * 如果是 self => 则表示给this起了一个别名，叫self，这在有内部类时用的比较多，
  */
trait RegistryNotify { this:Registry =>
  /**
    * 节点监听列表
    */
  private var nodeListener:Array[RegistryListener] = Array.empty[RegistryListener]
  /**
    * 添加节点监听器
    * @param listener 待添加的节点监听器
    */
  def subscribe(listener: RegistryListener):Unit = {
    nodeListener = listener +:  nodeListener
  }

  /**
    * 取消节点监听器
    * @param listener 待取消的节点
    */
  def unSubscribe(listener: RegistryListener):Unit = {
    nodeListener = nodeListener.filter(_==listener)
  }

  /**
    * 触发通知
    * @param node 触发通知的节点
    * @param event 触发通知的事件
    */
  def notify(node:Node,event:RegistryEvent):Unit = {
    nodeListener.filter(_.filter(node,event)).foreach(_.onEvent(node,event))
  }
}
