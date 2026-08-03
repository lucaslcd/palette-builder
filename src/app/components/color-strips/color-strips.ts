import { Component, computed, inject } from '@angular/core';
import { StateService } from '../../services/state.service';
import { ColorCopy } from '../color-copy/color-copy';

@Component({
  selector: 'pbu-color-strips',
  imports: [ColorCopy],
  templateUrl: './color-strips.html',
  styleUrl: './color-strips.css',
})
export class ColorStrips {
  stateService = inject(StateService)

  colors = computed(()=>this.stateService.params().colors)
}
