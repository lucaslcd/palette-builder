import { Component, computed, inject } from '@angular/core';
import { StateService } from '../../services/state.service';
import { ColorCopy } from '../color-copy/color-copy';

@Component({
  selector: 'pbu-palette-export',
  imports: [ColorCopy],
  templateUrl: './palette-export.html',
  styleUrl: './palette-export.css',
})
export class PaletteExport {
  stateService = inject(StateService)

  colors = computed(()=>this.stateService.params().colors)
}
