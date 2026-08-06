import { Component, computed, inject } from '@angular/core';
import { ColorState } from '../../services/color.state';
import { ColorCopy } from '../color-copy/color-copy';

@Component({
  selector: 'pbu-color-strips',
  imports: [ColorCopy],
  templateUrl: './color-strips.html',
  styleUrl: './color-strips.css',
})
export class ColorStrips {
  colorState = inject(ColorState)

  colors = computed(()=>this.colorState.state().colors)
}
