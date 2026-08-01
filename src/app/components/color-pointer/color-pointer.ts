import { Component, computed, inject, input } from '@angular/core';
import { UtilsService } from '../../services/utils.service';

@Component({
  selector: 'pbu-color-pointer',
  imports: [],
  templateUrl: './color-pointer.html',
  styleUrl: './color-pointer.css',
  host: {
    "[style.backgroundColor]": "color()",
    "[style.left]": "left()",
    "[style.top]": "top()",
  }
})
export class ColorPointer {
  utilsService = inject(UtilsService)

  hue = input.required<number>()
  saturation = input.required<number>()
  light = input.required<number>()

  color = computed(()=>this.utilsService.formatHsl(this.hue(), this.saturation(), this.light()))

  position = computed<number[]>(()=>{
    const angleRadians = Math.PI / 180 * this.hue(),
    c = Math.cos(angleRadians),
    s = Math.sin(angleRadians),
    x = this.light(), 
    y = 0;

    return [
      x * c - y * s,
      x * s + y * c 
    ]
  })

  left = computed(()=>{
    var perc = (this.position()[0] + 1) * 50
    return perc + "%"
  })
  top = computed(()=>{
    var perc = (this.position()[1] + 1) * 50
    return perc + "%"
  })
}
